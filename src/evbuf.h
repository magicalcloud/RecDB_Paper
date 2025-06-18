# include <vector>
# include <unordered_map>
# include <string>
# include <shared_mutex> 
# include <mutex> 

class valNode {
  public:
    std::vector<float> val;
    int refcount;
    valNode();
    valNode(int vsize);
    valNode(int vsize, int r);
    valNode(int r, std::vector<float>& v);
};

class vectorBuffer {
  public:
    int batchNum;
    std::unordered_map<std::string, valNode*> kv_map;
    std::shared_mutex kvmap_mtx;
    vectorBuffer();

    bool find(std::string key);

    void addExistVal(std::string key);

    void insert (std::string key, std::vector<float>& v);

    std::vector<std::vector<float>> respond(std::vector<std::vector<int>>& t_fs);

    void update(std::vector<std::vector<int>>& t_fs, std::vector<std::vector<std::vector<float>>>& udata);

    void update(std::vector<std::vector<int>>& t_fs);

    int addBatchNum(int batch);

    int getSize();

};