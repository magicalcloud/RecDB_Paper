# include "evbuf.h"
# include <iostream>

// valNode
valNode::valNode() {
    refcount = 0;
}
valNode::valNode(int vsize) {
    val = std::vector<float>(vsize);
    refcount = 1;
}
valNode::valNode(int vsize, int r) {
    val = std::vector<float>(vsize);
    refcount = r;
}
valNode::valNode(int r, std::vector<float>& v) {
    val = v;
    refcount = r;
}


// vectorBuffer
vectorBuffer::vectorBuffer() {
    batchNum = 0;
    kv_map = std::unordered_map<std::string, valNode*>();
}

bool vectorBuffer::find(std::string key) {
    std::unique_lock<std::shared_mutex> kvlock(kvmap_mtx);
    if (kv_map.find(key)!=kv_map.end()) {
        kv_map[key]->refcount++;
        kvlock.unlock();
        return true;
    }
    else {
        kvlock.unlock();
        return false;
    }
}

void vectorBuffer::addExistVal(std::string key) {
    std::unique_lock<std::shared_mutex> kvlock(kvmap_mtx);
    kv_map[key]->refcount++;
    kvlock.unlock();
}

int vectorBuffer::addBatchNum(int batch)
{
    std::unique_lock<std::shared_mutex> kvlock(kvmap_mtx);
    batchNum += batch;
    return batchNum;
    // kvlock.unlock();
}

int vectorBuffer::getSize()
{
    std::shared_lock<std::shared_mutex> kvlock(kvmap_mtx);
    int res = kv_map.size();
    kvlock.unlock();
    return res;
}

void vectorBuffer::insert (std::string key, std::vector<float>& v) {
    std::unique_lock<std::shared_mutex> kvlock(kvmap_mtx);
    if (kv_map.find(key)==kv_map.end()) {
        kv_map[key] = new valNode(1, v);
    }
    else {
        kv_map[key]->refcount++;
    }
    kvlock.unlock();
}

std::vector<std::vector<float>> vectorBuffer::respond(std::vector<std::vector<int>>& t_fs) {
    int t = t_fs.size();
    int f = t_fs[0].size();
    std::vector<std::vector<float>> res(t*f);
    std::unique_lock<std::shared_mutex> kvlock(kvmap_mtx);
    for (int i = 0; i < t; i++) {
        for (int j = 0; j < f; j++) {
            std::string key = std::to_string(i+1)+"-"+std::to_string(t_fs[i][j]);
            res[i*f+j] = kv_map[key]->val;
            if (kv_map[key]->refcount > 1) kv_map[key]->refcount--;
            else {
                auto it = kv_map.find(key);
                delete it->second;
                kv_map.erase(key);
            }
        }
    }
    batchNum--;
    kvlock.unlock();
    return res;

}

void vectorBuffer::update(std::vector<std::vector<int>>& t_fs, std::vector<std::vector<std::vector<float>>>& udata) {
    int t = t_fs.size();
    int f = t_fs[0].size();
    std::unique_lock<std::shared_mutex> kvlock(kvmap_mtx);
    for (int i = 0; i < t; i++) {
        for (int j = 0; j < f; j++) {
            std::string key = std::to_string(i+1)+"-"+std::to_string(t_fs[i][j]);
            if (kv_map.find(key)!=kv_map.end()) {
                kv_map[key]->val = udata[i][j];
            }
        }
    }
    kvlock.unlock();

}

void vectorBuffer::update(std::vector<std::vector<int>>& t_fs) {
    int t = t_fs.size();
    int f = t_fs[0].size();
    std::unique_lock<std::shared_mutex> kvlock(kvmap_mtx);
    for (int i = 0; i < t; i++) {
        for (int j = 0; j < f; j++) {
            std::string key = std::to_string(i+1)+"-"+std::to_string(t_fs[i][j]);
            if (kv_map.find(key)!=kv_map.end()) {
            }
        }
    }
    kvlock.unlock();

}