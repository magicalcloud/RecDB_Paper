# include "recdb.h"
# include <algorithm>
# include <thread>
# include <chrono>

HotMonitor::HotMonitor() {

}

HotAllocator::HotAllocator() {

}

void HotMonitor::IdentifyHotAndSort(std::vector<std::vector<std::vector<int>>>& mutliBatch, std::unordered_set<std::string>& waitingset, std::unordered_set<std::string>& hotkeyset, std::vector<batchKey>& prefetch_keys ){
    int prefetch_window_size = mutliBatch.size();
    int table_num = mutliBatch[0].size();
    int min_batch_size = mutliBatch[0][0].size();
    std::cout << prefetch_window_size << " " << table_num << " " << min_batch_size << std::endl;

    // 将其中的热key加前缀
    auto sorter_start_time = std::chrono::steady_clock::now();
    for (int iter = 0; iter < prefetch_window_size; iter++) {
        for (int t = 0; t < table_num; t++) {
            for (int b = 0; b < min_batch_size; b++) {
                std::string key_ = std::to_string(t+1)+"-"+std::to_string(mutliBatch[iter][t][b]);
                if (hotkeyset.find(key_)!=hotkeyset.end()) {
                    prefetch_keys.push_back({"X-"+key_, iter, t, b});
                }
                else {
                    prefetch_keys.push_back({key_, iter, t, b});
                }
            }
        }
    }
    auto addpre_end_time = std::chrono::steady_clock::now();

    // 排序
    std::sort(prefetch_keys.begin(), prefetch_keys.end(), [](const batchKey &a, const batchKey &b) {
      return a.key < b.key; // 从小到大排序
    });
    auto sorter_end_time = std::chrono::steady_clock::now();

    // 统计新的hotkey
    std::vector<hotKey> hotkeys;
    hotkeys.push_back({"0", 0});
    for (int h = 0; h < prefetch_keys.size(); h++) {
        if (prefetch_keys[h].key[0]=='X') continue;
            if (prefetch_keys[h].key==hotkeys[hotkeys.size()-1].key) {
                hotkeys[hotkeys.size()-1].num++;
            }
        else {
            hotkeys.push_back({prefetch_keys[h].key, 1});
        }
    }
    std::sort(hotkeys.begin(), hotkeys.end(), [](const hotKey &a, const hotKey &b) {
        return a.num > b.num; // 从大到小排序
    });

    // 将新的hotkey放进wait set
    for (int h = 0; h < hotkeys.size(); h++) {
        if (hotkeys[h].num > 1000) {
            if (hotkeyset.find(hotkeys[h].key)==hotkeyset.end()) {
                waitingset.insert(hotkeys[h].key);
            }
        }
    }
    auto iden_end_time = std::chrono::steady_clock::now();
    std::chrono::duration<double> addpre_time = addpre_end_time - sorter_start_time;
    std::chrono::duration<double> sorter_time = sorter_end_time - addpre_end_time;
    std::chrono::duration<double> iden_time = iden_end_time - sorter_end_time;
    // std::cout << "addpre_time  : " << addpre_time.count() << " seconds" << std::endl;
    // std::cout << "sorter_time  : " << sorter_time.count() << " seconds" << std::endl;
    // std::cout << "iden_time " << iden_time.count() << " seconds" << std::endl;
}


void HotAllocator::AddPrefix(std::vector<std::vector<int>>& sparse_input, std::unordered_set<std::string>& waitingset, std::unordered_set<std::string>& hotkeyset, std::vector<std::string>& write_keys) {
    int table_num = sparse_input.size();
    int min_batch_size = sparse_input[0].size();
    for (int t = 0; t < table_num; t++) {
        for (int b = 0; b < min_batch_size; b++) {
            std::string key_ = std::to_string(t+1)+"-"+std::to_string(sparse_input[t][b]);
            if (hotkeyset.find(key_)!=hotkeyset.end() || 
                waitingset.find(key_)!=waitingset.end()) {
                    write_keys.push_back("X-"+key_);
                }
            else {
                write_keys.push_back(key_);
            }
        }
    }
    // 排序
    std::sort(write_keys.begin(), write_keys.end(), [](const std::string &a, const std::string &b) {
      return a < b; // 从小到大排序
    });
    return ;
}

Preprocessor::Preprocessor() {
    keyMonitor = HotMonitor();
    keyAllocator = HotAllocator();
    is_prefetching = false;
    hot_set = std::unordered_set<std::string>();
    wait_set = std::unordered_set<std::string>();
    value_size = 9;
}

Preprocessor::Preprocessor(int vsize) {
    keyMonitor = HotMonitor();
    keyAllocator = HotAllocator();
    is_prefetching = false;
    hot_set = std::unordered_set<std::string>();
    wait_set = std::unordered_set<std::string>();
    value_size = vsize;
}

void Preprocessor::prefetchEVs(rocksdb::DB *db_ptr, vectorBuffer &VB, const rocksdb::ReadOptions &read_opts, bool &is_db_pref, std::vector<std::vector<std::vector<int>>> &inVB, std::shared_mutex& invbmtx)
{
    // 给db地read上锁 阻塞compaction线程
    std::unique_lock read_lck(rocksdb::DB::isread_mtx);

    // 先判断目前是否正在进行预取
    // 先检测热key，并排序
    // 用排序后的key order进行 LSM的读取 并将结果放进 EVBuffer

    if (is_prefetching) return;

    // 计时
    auto start_time = std::chrono::steady_clock::now();

    is_prefetching = true;
    is_db_pref = true;
    std::vector<batchKey> prefetchkeys;
    std::unique_lock<std::shared_mutex> uwaitlock(wait_set_mtx);
    std::shared_lock<std::shared_mutex> shotlock(hot_set_mtx);
    keyMonitor.IdentifyHotAndSort(multiBatch_copy, wait_set, hot_set, prefetchkeys);
    uwaitlock.unlock();
    shotlock.unlock();

    // 用排序后的key order进行 LSM的读取 并将结果放进 EVBuffer
    size_t floatsize = sizeof(float)*value_size;
    int prefetch_windows_size =  multiBatch_copy.size();
    int table_num =  multiBatch_copy[0].size();
    int min_batch_size =  multiBatch_copy[0][0].size();
    std::vector<std::thread> getfuncThreads;
    int thread_num = prefetch_windows_size; //table_num*2;
    int val_num_in_thread = min_batch_size/2;
    getfuncThreads.reserve(thread_num);
    for (size_t iter = 0; iter < thread_num; iter++) {
        getfuncThreads.emplace_back([&, iter]() {
            // 线程中的任务
            // std::cout << iter << std::endl;
            for (int i = 0; i < table_num*2; i++) {
                for (int j = 0; j < val_num_in_thread; j++) {
                    std::string s;
                    std::vector<float> decode_val = std::vector<float>(value_size);
                    // batchKey k = prefetchkeys[iter*prefetch_windows_size*26+i*val_num_in_thread+j];
                    std::string key_ = prefetchkeys[iter*min_batch_size*table_num+i*val_num_in_thread+j].key;
                    std::string VBkey_ = key_;
                    if (key_[0]=='X') {
                        VBkey_ = key_.substr(2);
                    }
                    if (VB.find(VBkey_)) VB.addExistVal(VBkey_);
                    else 
                    {
                        rocksdb::Status status = db_ptr->Get(read_opts, key_, &s);
                        if (!status.ok()) {
                            std::cout << "Get operation failed with unknown error: " << status.ToString() << " " << key_ << std::endl;
                        }
                        std::memcpy(decode_val.data(), s.data(), floatsize);
                        VB.insert(VBkey_, decode_val);
                        // std::cout << VB.kv_map.size() << std::endl;
                    }
                }
            }
        });
    }

    for (auto& thread_ : getfuncThreads) {
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    rocksdb::DB::emb_vec_buffer = VB.addBatchNum(prefetch_windows_size);
    read_lck.unlock();

    
    std::unique_lock<std::shared_mutex> inVB_lock(invbmtx);
    for (int i = 0; i < prefetch_windows_size; i++) {
        inVB.push_back(multiBatch_copy[i]);
    }
    inVB_lock.unlock();
    is_prefetching = false;
    is_db_pref = false;

    // 计时
    auto end_time = std::chrono::steady_clock::now();
    std::chrono::duration<double> elapsed_seconds = end_time - start_time;
    std::cout << "VBuffer size         : " << rocksdb::DB::emb_vec_buffer << " " << VB.getSize() << std::endl;
    std::cout << "group_rowIds.size()  : " << multiBatch_copy.size() << std::endl;
    std::cout << "prefect + decode time: " << elapsed_seconds.count() << " seconds" << std::endl;
}

void Preprocessor::prefetchEVs(rocksdb::DB* db_ptr, vectorBuffer& VB, const rocksdb::ReadOptions& read_opts, bool& is_db_pref, std::vector<std::vector<std::vector<int>>> multiBatch, std::vector<std::vector<std::vector<int>>>& inVB) {
    // 先判断目前是否正在进行预取
    // 先检测热key，并排序
    // 用排序后的key order进行 LSM的读取 并将结果放进 EVBuffer

    if (is_prefetching) return;
    is_prefetching = true;
    is_db_pref = true;
    std::vector<batchKey> prefetchkeys;
    std::unique_lock<std::shared_mutex> uwaitlock(wait_set_mtx);
    std::shared_lock<std::shared_mutex> shotlock(hot_set_mtx);
    keyMonitor.IdentifyHotAndSort(multiBatch, wait_set, hot_set, prefetchkeys);
    uwaitlock.unlock();
    shotlock.unlock();

    // 用排序后的key order进行 LSM的读取 并将结果放进 EVBuffer
    size_t floatsize = sizeof(float)*value_size;
    int prefetch_windows_size =  multiBatch.size();
    int table_num =  multiBatch[0].size();
    int min_batch_size =  multiBatch[0][0].size();
    std::vector<std::thread> getfuncThreads;
    int thread_num = prefetch_windows_size; //table_num*2;
    int val_num_in_thread = min_batch_size/2;
    getfuncThreads.reserve(thread_num);
    for (size_t iter = 0; iter < thread_num; iter++) {
        getfuncThreads.emplace_back([&, iter]() {
            // 线程中的任务
            for (int i = 0; i < table_num*2; i++) {
                for (int j = 0; j < val_num_in_thread; j++) {
                    std::string s;
                    std::vector<float> decode_val = std::vector<float>(value_size);
                    // batchKey k = prefetchkeys[iter*prefetch_windows_size*26+i*val_num_in_thread+j];
                    std::string key_ = prefetchkeys[iter*prefetch_windows_size*26+i*val_num_in_thread+j].key;
                    std::string VBkey_ = "";
                    if (key_[0]=='X') {
                        VBkey_ = key_.substr(2);
                    }
                    if (VB.find(VBkey_)) VB.addExistVal(VBkey_);
                    else 
                    {
                        rocksdb::Status status = db_ptr->Get(read_opts, key_, &s);
                        if (!status.ok()) {
                            std::cout << "Get operation failed with unknown error: " << status.ToString() << " " << key_ << std::endl;
                        }
                        std::memcpy(decode_val.data(), s.data(), floatsize);
                        VB.insert(VBkey_, decode_val);
                    }
                }
            }
        });

        for (auto& thread_ : getfuncThreads) {
            if (thread_.joinable()) {
                thread_.join();
            }
        }
    }
    
    for (int i = 0; i < prefetch_windows_size; i++) {
        inVB.push_back(multiBatch[i]);
    }
    is_prefetching = false;
    is_db_pref = false;
}

void Preprocessor::moveWaitToHot(std::vector<std::vector<int>> &sparse_input)
{
    int table_num = sparse_input.size();
    int min_batch_size = sparse_input[0].size();
    for (int t = 0; t < table_num; t++) {
        for (int b = 0; b < min_batch_size; b++) {
            std::string key_ = std::to_string(t+1)+"-"+std::to_string(sparse_input[t][b]);
            if (wait_set.find(key_)!=wait_set.end()) {
                wait_set.erase(key_);
                hot_set.insert(key_);
            }
        }
    }
    return ;
}

// RecDB
RecDB::RecDB() {
    db_ptr = nullptr;
    table_options = rocksdb::BlockBasedTableOptions();
    opts = rocksdb::Options();
    read_opts = rocksdb::ReadOptions();
    write_opts = rocksdb::WriteOptions();
    preprocessor.value_size = 9;
    value_size = 9;
    is_prefetching = false;
}

RecDB::RecDB(int vsize) {
    db_ptr = nullptr;
    table_options = rocksdb::BlockBasedTableOptions();
    opts = rocksdb::Options();
    read_opts = rocksdb::ReadOptions();
    write_opts = rocksdb::WriteOptions();
    preprocessor.value_size = vsize;
    value_size = vsize;
    is_prefetching = false;
}

void RecDB::Open(const std::string& name) {
    opts.create_if_missing = true;
    if (db_ptr != nullptr) {
        throw std::invalid_argument("db has been opened");
    }
    // class MyListener : public EventListener {
    //   public:
    //     void OnCompactionCompleted(DB* db, const CompactionJobInfo& info)  {
    //     //   std::cout << "Compaction from level " << info.base_input_level << " to level " << info.output_level << std::endl;
    //         // 统计被删除的字节数（approximate estimate）
    //     if (info.stats.total_input_bytes > info.stats.total_output_bytes) {
    //         uint64_t deleted_bytes = info.stats.total_input_bytes - info.stats.total_output_bytes;
    //         std::cout << info.stats.total_input_bytes << " " << info.stats.total_output_bytes << " Deleted keys size  : " << deleted_bytes << " bytes" << std::endl;
    //         std::cout << "deleted_bytes/input: " << (double)deleted_bytes/info.stats.total_input_bytes << std::endl;
    //     }
    //     // 获取 compaction 耗时（单位：微秒）
    //     auto _end_time = std::chrono::steady_clock::now();
    //     std::chrono::duration<double> prefect_compact_t_ = _end_time - prefect_begin_time;
    //     uint64_t compaction_time_micros = info.stats.elapsed_micros;
    //     std::cout << "compaction time: " << compaction_time_micros << std::endl;
    //     std::unique_lock<std::shared_mutex> tcflck(total_compaction_filesize_mtx);
    //     if (info.stats.total_output_bytes!=0) {
    //         if (compaction_time_micros>prefect_compact_t_.count()*1000000) {
    //         total_compaction_filesize += info.stats.total_output_bytes*((prefect_compact_t_.count()*1000000)/compaction_time_micros);
    //         pre_compaction_filesize += info.stats.total_output_bytes*((compaction_time_micros-prefect_compact_t_.count()*1000000)/compaction_time_micros);
    //         }
    //         else total_compaction_filesize += info.stats.total_output_bytes;
    //     }
    //     tcflck.unlock();
    //     }
    // };
    // opts.listeners.emplace_back(std::make_shared<MyListener>());

    opts.max_open_files = 1;
    opts.statistics = rocksdb::CreateDBStatistics(); 
    opts.write_buffer_size = 6538152000;
    opts.max_write_buffer_number = 2;
    opts.max_background_flushes = 40;
    opts.allow_concurrent_memtable_write = true;

    // 将l0放进内存文件系统文件夹
    opts.db_paths = {
        {name, 536870912000},  // 其他层的存储路径（100 GB的空间大小作为示例）
        {"/mnt/nvme0n1/gm/ev-table-l0", 1073741824}  // 设置L0层的存储路径（10 GB的空间大小作为示例）
    };
    // ===

    // filter=====
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
    table_options.cache_index_and_filter_blocks = true;
    opts.level0_file_num_compaction_trigger = 8;
    opts.max_background_compactions = 40; 
    opts.max_background_jobs = 80; 
    opts.max_subcompactions = 40;

    opts.compression_per_level.resize(11);
    for (int i = 0; i < 11; ++i) {
        opts.compression_per_level[i] = rocksdb::CompressionType::kNoCompression;
    }
    std::shared_ptr<rocksdb::Cache> cache1 = rocksdb::NewLRUCache(2268108864, 6, true, 1);
    table_options.block_cache = cache1;
    opts.optimize_filters_for_hits = true;
    table_options.pin_top_level_index_and_filter = true;
    table_options.cache_index_and_filter_blocks_with_high_priority = true;
    table_options.enable_index_compression = false;

    auto table_factory = rocksdb::NewBlockBasedTableFactory(table_options);
    opts.table_factory.reset(table_factory);

    rocksdb::Status st =  rocksdb::DB::Open(opts, name, &db_ptr);
    if (!st.ok()) {
        std::cout << "Error opening database: " << st.ToString() << std::endl;
    }
    return ;
}

void RecDB::InitDataBase() {
    size_t byteSize = value_size*sizeof(float);
    std::vector<int> feature_nums = {10131227,10131227,10131227,10131227,10131227,10131227,10131227,10131227,10131227,10131227,10131227,10131227,10131227,10131227,10131227,10131227,10131227,10131227,10131227,10131227,10131227,10131227,10131227,10131227,10131227,10131227};
    // multithread
    std::vector<std::thread> getfuncThreads;
    getfuncThreads.reserve(26);
    for (int t = 0; t < 26; t++) {
        int t_ = t;
        size_t byteSize_ = byteSize;
        int feature_num = feature_nums[t];
        getfuncThreads.emplace_back([&, t_, byteSize_, feature_num ]() {
            int batch_count = 0;
            rocksdb::WriteBatch batch = rocksdb::WriteBatch();
            for (int fid = 0; fid <= feature_num-1; fid++){
                if (batch_count < 1000000) {
                    std::string key = std::to_string(t_+1)+"-"+std::to_string(fid);
                    std::string val(byteSize_, '\0');
                    std::vector<float> val_f = std::vector<float>(value_size, 0.0f);
                    std::memcpy(&val[0], val_f.data(), byteSize_);
                    batch.Put(key, val);
                    batch_count++;
                }
                else if (batch_count == 1000000) {
                    std::string key = std::to_string(t_+1)+"-"+std::to_string(fid);
                    std::string val(byteSize_, '\0');
                    std::vector<float> val_f = std::vector<float>(value_size, 0.0f);
                    std::memcpy(&val[0], val_f.data(), byteSize_);
                    batch.Put(key, val);
                    db_ptr->Write(write_opts, &batch);
                    batch_count = 0;
                    batch = rocksdb::WriteBatch();
                }
                if (fid == feature_num-1) {
                    db_ptr->Write(write_opts, &batch);
                    batch_count = 0;
                    batch = rocksdb::WriteBatch();
                }
                if (fid%10000000 == 0) std::cout << "write 10 batch " << t_ << " " << fid/10000000 << std::endl;
            }
            std::cout << "finish table write" << t_+1 << std::endl;
        });
    }
    for (auto& thread_ : getfuncThreads) {
        if (thread_.joinable()) {
            thread_.join();
        }
    }
}

bool RecDB::prefetchv1(std::vector<std::vector<std::vector<int>>> &multiBatch)
{
    // 如果正在做compaction return
    std::shared_lock is_comp_lck(rocksdb::DB::iscomp_mtx);
    if (rocksdb::DB::is_compaction > 0) {
        is_comp_lck.unlock();
        return false;
    }
    is_comp_lck.unlock();

    // 如果正在做prefetch return
    if (preprocessor.is_prefetching) return false;

    preprocessor.multiBatch_copy = multiBatch;
    std::thread perfectmultbatchkey_t([this, &multiBatch]() {
        this->preprocessor.prefetchEVs(this->db_ptr, this->VB, this->read_opts, this->is_prefetching, this->inVB, this->inVB_mtx);
    });
    perfectmultbatchkey_t.detach();
    return true;
}

bool RecDB::prefetch(std::vector<std::vector<std::vector<int>>>& multiBatch, std::vector<std::vector<std::vector<int>>>& inVB){
    // 如果正在做compaction return
    std::shared_lock is_comp_lck(rocksdb::DB::iscomp_mtx);
    if (rocksdb::DB::is_compaction > 0) {
        is_comp_lck.unlock();
        return false;
    }
    is_comp_lck.unlock();

    // 如果正在做prefetch return
    if (preprocessor.is_prefetching) return false;
    preprocessor.multiBatch_copy = multiBatch;
    std::thread perfectmultbatchkey_t([this, &multiBatch, &inVB]() {
        this->preprocessor.prefetchEVs(this->db_ptr, this->VB, this->read_opts, this->is_prefetching, inVB, this->inVB_mtx);
    });
    perfectmultbatchkey_t.detach();
    return true;
}

std::vector<std::vector<float>> RecDB::respond(std::vector<std::vector<int>>& t_fs) {
    return VB.respond(t_fs);
}

// void RecDB::updateDB(std::vector<std::vector<int>> &t_fs, std::vector<std::vector<std::vector<float>>> &udata)
// {
//     std::vector<std::string> write_keys;
//     std::shared_lock<std::shared_mutex> shotlock(preprocessor.hot_set_mtx);
//     std::shared_lock<std::shared_mutex> swaitlock(preprocessor.wait_set_mtx);
//     auto addpre_start_time = std::chrono::steady_clock::now();
//     preprocessor.keyAllocator.AddPrefix(t_fs, preprocessor.wait_set, preprocessor.hot_set, write_keys);
//     auto addpre_end_time = std::chrono::steady_clock::now();
//     shotlock.unlock();
//     swaitlock.unlock();
//     std::chrono::duration<double> add_time = addpre_end_time - addpre_start_time;
//     // std::cout << "addpre_time: " << add_time.count() << " seconds" << std::endl;

//     rocksdb::WriteBatch wbatch;
//     size_t byteSize = value_size*sizeof(float);
//     std::string prekey = "";
//     int count = 0;

//     for (std::string key_ : write_keys) {
//         if (key_==prekey) {
//             count++;
//             continue;
//         }
//         std::string val(byteSize, '\0');
//         // std::vector<float> val_j = std::vector<float>(value_size, 0.0f);
//         std::memcpy(&val[0], udata[count/udata[0].size()][count%udata[0].size()].data(), byteSize);
//         prekey = key_;
//         wbatch.Put(key_, val);
//         count++;
//     }
//     auto encode_end_time = std::chrono::steady_clock::now();
//     db_ptr->Write(write_opts, &wbatch);
//     auto write_end_time = std::chrono::steady_clock::now();
//     std::chrono::duration<double> write_time = write_end_time - encode_end_time;
//     std::chrono::duration<double> encode_time = encode_end_time - addpre_end_time;
//     std::cout << "encode_time: " << encode_time.count() << " seconds" << std::endl;
//     std::cout << "write_time: " << write_time.count() << " seconds" << std::endl;

//     std::unique_lock<std::shared_mutex> uhotlock(preprocessor.hot_set_mtx);
//     std::unique_lock<std::shared_mutex> uwaitlock(preprocessor.wait_set_mtx);
//     preprocessor.moveWaitToHot(t_fs);
//     uhotlock.unlock();
//     uwaitlock.unlock();
// }

void RecDB::updateDB(std::vector<std::vector<int>> &t_fs, std::vector<std::vector<std::vector<float>>> &udata)
{
    std::vector<std::string> write_keys;
    std::shared_lock<std::shared_mutex> shotlock(preprocessor.hot_set_mtx);
    std::shared_lock<std::shared_mutex> swaitlock(preprocessor.wait_set_mtx);
    auto addpre_start_time = std::chrono::steady_clock::now();
    preprocessor.keyAllocator.AddPrefix(t_fs, preprocessor.wait_set, preprocessor.hot_set, write_keys);
    auto addpre_end_time = std::chrono::steady_clock::now();
    shotlock.unlock();
    swaitlock.unlock();
    std::chrono::duration<double> add_time = addpre_end_time - addpre_start_time;
    // std::cout << "addpre_time: " << add_time.count() << " seconds" << std::endl;

    rocksdb::WriteBatch wbatch;
    size_t byteSize = value_size*sizeof(float);
    std::string prekey = "";
    int count = 0;

    std::vector<std::thread> writefuncThreads;
    writefuncThreads.reserve(5);
    for (int t = 0; t < 5; t++) {
        writefuncThreads.emplace_back([&, t, &write_keys, &udata]() {
            std::string prekey = "";
            int count = t*(write_keys.size()/5);
            rocksdb::WriteBatch wbatch;
            for (int index = count; index < write_keys.size() && index < (t+1)*(write_keys.size()/5); index++) {
                std::string key_ = write_keys[index];
                if (key_==prekey) {
                    continue;
                }
                std::string val(byteSize, '\0');
                // std::vector<float> val_j = std::vector<float>(value_size, 0.0f);
                std::memcpy(&val[0], udata[index/udata[0].size()][index%udata[0].size()].data(), byteSize);
                prekey = key_;
                wbatch.Put(key_, val);
            }
            db_ptr->Write(write_opts, &wbatch);
        });
    }

    for (std::string key_ : write_keys) {
        if (key_==prekey) {
            count++;
            continue;
        }
        std::string val(byteSize, '\0');
        // std::vector<float> val_j = std::vector<float>(value_size, 0.0f);
        std::memcpy(&val[0], udata[count/udata[0].size()][count%udata[0].size()].data(), byteSize);
        prekey = key_;
        wbatch.Put(key_, val);
        count++;
    }

    auto encode_end_time = std::chrono::steady_clock::now();
    db_ptr->Write(write_opts, &wbatch);
    auto write_end_time = std::chrono::steady_clock::now();
    std::chrono::duration<double> write_time = write_end_time - encode_end_time;
    std::chrono::duration<double> encode_time = encode_end_time - addpre_end_time;
    std::cout << "encode_time: " << encode_time.count() << " seconds" << std::endl;
    std::cout << "write_time: " << write_time.count() << " seconds" << std::endl;

    std::unique_lock<std::shared_mutex> uhotlock(preprocessor.hot_set_mtx);
    std::unique_lock<std::shared_mutex> uwaitlock(preprocessor.wait_set_mtx);
    preprocessor.moveWaitToHot(t_fs);
    uhotlock.unlock();
    uwaitlock.unlock();
}

void RecDB::updateVB(std::vector<std::vector<int>> &t_fs, std::vector<std::vector<std::vector<float>>> &udata){
    VB.update(t_fs, udata);
}

std::vector<std::vector<int>> RecDB::getCurrentInput(int curr)
{
    std::shared_lock<std::shared_mutex> inVB_lock(inVB_mtx);
    if (inVB.size() <= curr) return std::vector<std::vector<int>>();
    return inVB[curr];
}
