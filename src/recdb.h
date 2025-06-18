# include <pybind11/pybind11.h>
# include <pybind11/stl.h>  // 用于 STL 容器的支持
# include <pybind11/complex.h>  // 用于 STL 容器的支持
# include <pybind11/functional.h>
# include <pybind11/chrono.h>
# include <iostream>
# include <string>
# include <unordered_map>
# include <unordered_set>
# include <vector>
# include <queue>
# include <rocksdb/db.h>
# include <rocksdb/status.h>
# include <rocksdb/options.h>
# include <rocksdb/statistics.h>
# include <rocksdb/iterator.h>
# include <rocksdb/slice.h>
# include <rocksdb/write_batch.h>
# include <rocksdb/table.h>
# include <rocksdb/filter_policy.h>
# include <rocksdb/cache.h>
# include <rocksdb/advanced_cache.h>
# include <rocksdb/snapshot.h>
# include <utilities/merge_operators.h>
# include <rocksdb/utilities/transaction_db.h>
# include <rocksdb/utilities/transaction.h>
# include <rocksdb/utilities/write_batch_with_index.h>
# include "evbuf.h"

namespace py = pybind11;

struct batchKey{
  std::string key;
  int batch_id;
  int table_id;
  int feature_id;
};

struct hotKey {
  std::string key;
  int num;
};

// 功能包括：
// 1. 给定lookahead, 并判断哪些key是热key，并将新的hotkey放进waitingallocated。
class HotMonitor {
  public:
    HotMonitor();
    void IdentifyHotAndSort(std::vector<std::vector<std::vector<int>>>& mutliBatch, std::unordered_set<std::string>& waitingset, std::unordered_set<std::string>& hotkeyset, std::vector<batchKey>& prefetch_keys );
};

// 功能：
// 1. 给write request中的热key 加前缀
class HotAllocator {
  public:
    HotAllocator();
    void AddPrefix(std::vector<std::vector<int>>& sparse_input, std::unordered_set<std::string>& waitingset, std::unordered_set<std::string>& hotkeyset, std::vector<std::string>& write_keys);
    
};

// 功能包括：
// 1. 启动预取； 2. 响应一次迭代emb lookup；3. 写回一个batch的emb update
// 1. 需要使用hotkeymonitor，hotkeyallocator
// 2. 
class Preprocessor {
  public:
    HotMonitor keyMonitor;
    HotAllocator keyAllocator;
    bool is_prefetching;
    std::unordered_set<std::string> hot_set;
    std::unordered_set<std::string> wait_set;
    std::shared_mutex hot_set_mtx;
    std::shared_mutex wait_set_mtx;
    int value_size;
    std::vector<std::vector<std::vector<int>>> multiBatch_copy;
    // 启动预取
    Preprocessor();
    Preprocessor(int vsize);
    void prefetchEVs(rocksdb::DB* db_ptr, vectorBuffer& VB, const rocksdb::ReadOptions& read_opts, bool& is_pref, std::vector<std::vector<std::vector<int>>>& inVB, std::shared_mutex& invbmtx);
    void prefetchEVs(rocksdb::DB* db_ptr, vectorBuffer& VB, const rocksdb::ReadOptions& read_opts, bool& is_pref, std::vector<std::vector<std::vector<int>>> mutliBatch, std::vector<std::vector<std::vector<int>>>& inVB);
    void moveWaitToHot(std::vector<std::vector<int>>& sparse_input);
};

class RecDB {
  public:
    rocksdb::DB* db_ptr;
    vectorBuffer VB;
    rocksdb::BlockBasedTableOptions table_options;
    rocksdb::Options opts;
    rocksdb::ReadOptions read_opts;
    rocksdb::WriteOptions write_opts;
    Preprocessor preprocessor;
    int value_size;
    bool is_prefetching;
    std::shared_mutex inVB_mtx;
    std::vector<std::vector<std::vector<int>>> inVB;

    RecDB();
    RecDB(int vsize);
    void Open(const std::string& name);
    void InitDataBase();
    bool prefetchv1(std::vector<std::vector<std::vector<int>>>& mutliBatch);
    bool prefetch(std::vector<std::vector<std::vector<int>>>& mutliBatch, std::vector<std::vector<std::vector<int>>>& inVB);
    std::vector<std::vector<float>> respond(std::vector<std::vector<int>>& t_fs);
    void updateDB(std::vector<std::vector<int>>& t_fs, std::vector<std::vector<std::vector<float>>>& udata);
    void updateVB(std::vector<std::vector<int>>& t_fs, std::vector<std::vector<std::vector<float>>>& udata);
    std::vector<std::vector<int>> getCurrentInput(int curr);
};

// 将 RecDB 类暴露给 Python
PYBIND11_MODULE(rocksdb_extension, m) {
    py::class_<RecDB>(m, "RecDB")
        .def(py::init<>())
        .def(py::init<int>())
        .def("Open", &RecDB::Open)
        .def("InitDataBase", &RecDB::InitDataBase)
        .def("prefetch", &RecDB::prefetchv1)
        .def("respond", &RecDB::respond)
        .def("updateDB", &RecDB::updateDB)
        .def("updateVB", &RecDB::updateVB)
        .def("getCurrentInput", &RecDB::getCurrentInput)
        .def_readwrite("isprefetching", &RecDB::is_prefetching);
}