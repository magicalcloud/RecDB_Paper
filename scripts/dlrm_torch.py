import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, Dataset
import numpy as np
import random
import time

import recdb
torch.set_num_threads(1)

mini_batch_size = 1024

class DLRM(nn.Module):
    def __init__(self, num_continuous_features, num_categorical_features, embedding_dim, hidden_units):
        super(DLRM, self).__init__()

        # 嵌入层
        self.embeddings = nn.ModuleList(
            [nn.Embedding(num_categories, embedding_dim) for num_categories in num_categorical_features]
        )

        # 连续特征的线性变换
        self.continuous_fc = nn.Sequential(
            nn.Linear(num_continuous_features, hidden_units[0]),
            nn.ReLU()
        )

        # MLP层
        self.mlp = nn.Sequential(
            nn.Linear(sum([embedding_dim for _ in num_categorical_features]) + hidden_units[0], hidden_units[1]),
            nn.ReLU(),
            nn.Linear(hidden_units[1], 1),
            nn.Sigmoid()  # 输出二分类概率
        )

    def forward(self, continuous_features, categorical_features):
        # 处理连续特征
        cont_out = self.continuous_fc(continuous_features)

        # 处理类别特征
        emb_out = [embedding(categorical_features[:, i]) for i, embedding in enumerate(self.embeddings)]
        cat_out = torch.cat(emb_out, dim=1)

        # 拼接连续特征和类别特征
        x = torch.cat([cont_out, cat_out], dim=1)

        # 通过MLP层
        return self.mlp(x)

class DLRMDataset(Dataset):
    def __init__(self, continuous_data, categorical_data, labels):
        self.continuous_data = continuous_data
        self.categorical_data = categorical_data
        self.labels = labels

    def __len__(self):
        return len(self.labels)

    def __getitem__(self, idx):
        return (self.continuous_data[idx], self.categorical_data[idx], self.labels[idx])
    
def gen_inputs( table_num, min_batch_size):
    # 初始化三维列表 totalSparseInputs
    global totalSparseInputs
    totalSparseInputs = [[[0 for _ in range(min_batch_size)] for _ in range(table_num)] for _ in range(1024)]

    # 填充数据
    for i in range(1024):
        for j in range(table_num):
            for k in range(min_batch_size):
                prob = random.randint(0, 100)
                if prob < 99:
                    # 50% 的概率生成 0 - 1000 之间的随机数
                    totalSparseInputs[i][j][k] = random.randint(0, 1000)
                else:
                    # 50% 的概率生成 1001 - 101311 之间的随机数
                    totalSparseInputs[i][j][k] = random.randint(1001, 101311)
                    
def gen_inputs_fromfile():
    global totalSparseInputs
    totalSparseInputs = np.fromfile('../sparse_inputs_1024.bin', dtype=np.int64).reshape((38371, 26, mini_batch_size))

def get_inputs(lookahead_winsize, prefetch_num):

    # 填充数据
    multiBatchInputs = totalSparseInputs[(prefetch_num-1)*lookahead_winsize:(prefetch_num)*lookahead_winsize]

    return multiBatchInputs

def train_model(model, dataloader, criterion, optimizer, num_epochs=10):
    prefetch_num = 1
    iter_num = 0
    for epoch in range(10000):
        model.train()
        running_loss = 0.0
        correct_preds = 0
        total_preds = 0

        for cont_data, cat_data, labels in dataloader:
            
            if (not rec_db.isprefetching):
                multi_batch = get_inputs(1024, prefetch_num)
                prefetch_num+=1
                rec_db.prefetch(multi_batch)
            
            curr_input = rec_db.getCurrentInput(iter_num)
            if (len(curr_input)==0):
                continue
            emb_weights = rec_db.respond(curr_input)
            
            for t in range(len(model.embeddings)):
                for i in range(1, len(model.embeddings[0].weight)):
                    model.embeddings[t].weight.data[i] = torch.tensor(emb_weights[t*(i-1)])
            
            for t in range(len(curr_input)):
                dbmem_map = np.zeros(10131227)
                count = 1
                for i in range(len(curr_input[0])):
                    if (dbmem_map[curr_input[t][i]]==0):
                        cat_data[i][t] = count
                        dbmem_map[curr_input[t][i]] = count
                        count+=1
                    else:
                        cat_data[i][t] = dbmem_map[curr_input[t][i]]
            
            optimizer.zero_grad()

            # 前向传播
            outputs = model(cont_data, cat_data)

            # 计算损失
            loss = criterion(outputs.squeeze(), labels.float())
            loss.backward()
            optimizer.step()
            udata = []
            for t in range(len(model.embeddings)):
                udata_t = []
                for i in range(1, len(model.embeddings[0].weight)):
                    udata_t.append(model.embeddings[t].weight[i].detach().numpy())
                udata.append(udata_t)
            
            update_t_begin = time.time()
            rec_db.updateDB(curr_input, udata)
            update_db_end = time.time()
            rec_db.updateVB(curr_input, udata)
            update_t_end = time.time()
            if (iter_num%123==0):
                print("db update: ", update_db_end-update_t_begin, "s")
                print("vb update: ", update_t_end-update_db_end, "s")

            # 统计损失
            running_loss += loss.item()

            # 计算准确率
            preds = (outputs.squeeze() > 0.5).float()
            correct_preds += (preds == labels).sum().item()
            total_preds += labels.size(0)
            
            iter_num += 1

        # avg_loss = running_loss / len(dataloader)
        # accuracy = correct_preds / total_preds

        # print(f"Epoch {epoch+1}/{num_epochs}, Loss: {avg_loss:.4f}, Accuracy: {accuracy:.4f}")

# 模拟数据
num_samples = 102400
num_continuous_features = 13
num_categorical_features = [mini_batch_size+1 for _ in range(26)]  # 每个类别特征的不同类别数
embedding_dim = 18
hidden_units = [8, 4]

# 随机生成数据
continuous_data = torch.randn(num_samples, num_continuous_features)
categorical_data = torch.randint(0, 10, (num_samples, len(num_categorical_features)))
labels = torch.randint(0, 1, (num_samples,))

# 创建数据集和数据加载器
dataset = DLRMDataset(continuous_data, categorical_data, labels)
dataloader = DataLoader(dataset, batch_size=mini_batch_size, shuffle=True)

# 创建模型
model = DLRM(num_continuous_features, num_categorical_features, embedding_dim, hidden_units)

# 损失函数和优化器
criterion = nn.BCELoss()
optimizer = optim.Adam(model.parameters(), lr=0.001)

# 创建 RecDB 实例
rec_db = recdb.RecDB(18)

# 调用 RecDB 的方法
rec_db.Open("/mnt/nvme0n1/gm/ev-table-all.kaggledb18")

# gen_inputs(26, 32)
gen_inputs_fromfile()

# 训练模型
train_model(model, dataloader, criterion, optimizer, num_epochs=10)
