import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, Dataset
import numpy as np
import random
import time

import recdb
torch.set_num_threads(1)

mini_batch_size = 32

# DeepFM 模型
class DeepFM(nn.Module):
    def __init__(self, 
                 num_fea_size,
                 cate_fea_uniques,
                 emb_size=8,
                 hidden_dims=[256, 128],
                 num_classes=1,
                 dropout=[0.2, 0.2]):
        '''
        :param cate_fea_uniques:
        :param num_fea_size: 数字特征  也就是连续特征
        :param emb_size:
        :param hidden_dims:
        :param num_classes:
        :param dropout:
        '''
        super(DeepFM, self).__init__()
        self.cate_fea_size = len(cate_fea_uniques)
        self.num_fea_size = num_fea_size

        # DeepFM
        # dense特征一阶表示
        if self.num_fea_size != 0:
            self.fm_1st_order_dense = nn.Linear(self.num_fea_size, 1)

        # sparse特征一阶表示
        self.fm_1st_order_sparse_emb = nn.ModuleList([
            nn.Embedding(voc_size, 1) for voc_size in cate_fea_uniques
        ])

        # sparse特征二阶表示
        self.embeddings = nn.ModuleList([
            nn.Embedding(voc_size, emb_size) for voc_size in cate_fea_uniques
        ])

        # DNN部分
        self.dense_linear = nn.Linear(self.num_fea_size, self.cate_fea_size * emb_size)  # # 数值特征的维度变换到FM输出维度一致
        self.relu = nn.ReLU()

        self.all_dims = [self.cate_fea_size * emb_size] + hidden_dims

        for i in range(1, len(self.all_dims)):
            setattr(self, 'linear_' + str(i), nn.Linear(self.all_dims[i-1], self.all_dims[i]))
            setattr(self, 'batchNorm_' + str(i), nn.BatchNorm1d(self.all_dims[i]))
            setattr(self, 'activation_' + str(i), nn.ReLU())
            setattr(self, 'dropout_' + str(i), nn.Dropout(dropout[i-1]))

        self.dnn_linear = nn.Linear(hidden_dims[-1], num_classes)
        self.sigmoid = nn.Sigmoid()

    def forward(self, X_sparse, X_dense=None):
        """
        X_sparse: sparse_feature [batch_size, sparse_feature_num]
        X_dense: dense_feature  [batch_size, dense_feature_num]
        """
        """FM部分"""
        # 一阶  包含sparse_feature和dense_feature的一阶
        fm_1st_sparse_res = [emb(X_sparse[:, i].unsqueeze(1)).view(-1, 1)
                             for i, emb in enumerate(self.fm_1st_order_sparse_emb)]  # sparse特征嵌入成一维度
        fm_1st_sparse_res = torch.cat(fm_1st_sparse_res, dim=1)  # torch.Size([2, 26])
        fm_1st_sparse_res = torch.sum(fm_1st_sparse_res, 1,  keepdim=True)  # [bs, 1] 将sparse_feature通过全连接并相加整成一维度

        if X_dense is not None:
            fm_1st_dense_res = self.fm_1st_order_dense(X_dense)   # 将dense_feature压到一维度
            fm_1st_part = fm_1st_sparse_res + fm_1st_dense_res
        else:
            fm_1st_part = fm_1st_sparse_res   # [bs, 1]

        # 二阶
        fm_2nd_order_res = [emb(X_sparse[:, i].unsqueeze(1)) for i, emb in enumerate(self.embeddings)]
        fm_2nd_concat_1d = torch.cat(fm_2nd_order_res, dim=1)  # batch_size, sparse_feature_nums, emb_size
        # print(fm_2nd_concat_1d.size())   # torch.Size([2, 26, 8])

        # 先求和再平方
        sum_embed = torch.sum(fm_2nd_concat_1d, 1)  # batch_size, emb_size
        square_sum_embed = sum_embed * sum_embed   # batch_size, emb_size

        # 先平方再求和
        square_embed = fm_2nd_concat_1d * fm_2nd_concat_1d  # [bs, n, emb_size]
        sum_square_embed = torch.sum(square_embed, 1)  # [bs, emb_size]

        # 相减除以2
        sub = square_sum_embed - sum_square_embed
        sub = sub * 0.5   # batch_size, embed_size

        # 再求和
        fm_2nd_part = torch.sum(sub, 1, keepdim=True)   # batch_size, 1

        """DNN部分"""
        dnn_out = torch.flatten(fm_2nd_concat_1d, 1)   # [bs, n * emb_size]

        if X_dense is not None:
            dense_out = self.relu(self.dense_linear(X_dense))  # batch_size, sparse_feature_num * emb_size
            dnn_out = dnn_out + dense_out   # batch_size, sparse_feature_num * emb_size

        # 从sparse_feature_num * emb_size 维度 转为 sparse_feature_num * emb_size 再转为 256
        # print(self.all_dims)   # [208, 256, 128]
        for i in range(1, len(self.all_dims)):
            dnn_out = getattr(self, 'linear_' + str(i))(dnn_out)
            dnn_out = getattr(self, 'batchNorm_' + str(i))(dnn_out)
            dnn_out = getattr(self, 'activation_' + str(i))(dnn_out)
            dnn_out = getattr(self, 'dropout_' + str(i))(dnn_out)
        dnn_out = self.dnn_linear(dnn_out)   # batch_size, 1
        out = fm_1st_part + fm_2nd_part + dnn_out   # [bs, 1]
        out = self.sigmoid(out)
        return out

# Dataset类
class DLRMDataset(Dataset):
    def __init__(self, continuous_data, categorical_data, labels):
        self.continuous_data = continuous_data
        self.categorical_data = categorical_data
        self.labels = labels

    def __len__(self):
        return len(self.labels)

    def __getitem__(self, idx):
        return (self.continuous_data[idx], self.categorical_data[idx], self.labels[idx])

# 生成输入数据
def gen_inputs(table_num, min_batch_size):
    global totalSparseInputs
    totalSparseInputs = [[[0 for _ in range(min_batch_size)] for _ in range(table_num)] for _ in range(1024)]
    for i in range(1024):
        for j in range(table_num):
            for k in range(min_batch_size):
                prob = random.randint(0, 100)
                if prob < 99:
                    totalSparseInputs[i][j][k] = random.randint(0, 1000)
                else:
                    totalSparseInputs[i][j][k] = random.randint(1001, 101311)

def gen_inputs_fromfile():
    global totalSparseInputs
    totalSparseInputs = np.fromfile('../sparse_inputs_32.bin', dtype=np.int64).reshape((102400, 26, mini_batch_size))

def get_inputs(lookahead_winsize, prefetch_num):
    multiBatchInputs = totalSparseInputs[(prefetch_num - 1) * lookahead_winsize:(prefetch_num) * lookahead_winsize]
    return multiBatchInputs

# 训练模型
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
                prefetch_num += 1
                rec_db.prefetch(multi_batch)

            curr_input = rec_db.getCurrentInput(iter_num)
            if len(curr_input) == 0:
                continue
            emb_weights = rec_db.respond(curr_input)

            for t in range(len(model.embeddings)):
                for i in range(1, len(model.embeddings[0].weight)):
                    model.embeddings[t].weight.data[i] = torch.tensor(emb_weights[t * (i - 1)])

            for t in range(len(curr_input)):
                dbmem_map = np.zeros(10131227)
                count = 1
                for i in range(len(curr_input[0])):
                    if dbmem_map[curr_input[t][i]] == 0:
                        cat_data[i][t] = count
                        dbmem_map[curr_input[t][i]] = count
                        count += 1
                    else:
                        cat_data[i][t] = dbmem_map[curr_input[t][i]]

            optimizer.zero_grad()

            # 前向传播
            # outputs = model(cont_data, cat_data)
            outputs = model(cat_data)

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
            if (iter_num % 123 == 0):
                print("db update: ", update_db_end - update_t_begin, "s")
                print("vb update: ", update_t_end - update_db_end, "s")

            # 统计损失
            running_loss += loss.item()

            # 计算准确率
            preds = (outputs.squeeze() > 0.5).float()
            correct_preds += (preds == labels).sum().item()
            total_preds += labels.size(0)

            iter_num += 1

# 模拟数据
num_samples = 102400
num_continuous_features = 0
num_categorical_features = [mini_batch_size + 1 for _ in range(26)]  # 每个类别特征的不同类别数
embedding_dim = 18
hidden_units = [8, 4]

# 随机生成数据
continuous_data = torch.randn(num_samples, num_continuous_features)
categorical_data = torch.randint(0, 10, (num_samples, len(num_categorical_features)))
labels = torch.randint(0, 1, (num_samples,))

# 创建数据集和数据加载器
dataset = DLRMDataset(continuous_data, categorical_data, labels)
dataloader = DataLoader(dataset, batch_size=mini_batch_size, shuffle=True)

# 创建 DeepFM 模型
model = DeepFM(num_continuous_features, num_categorical_features, embedding_dim, hidden_units)

# 损失函数和优化器
criterion = nn.BCELoss()
optimizer = optim.Adam(model.parameters(), lr=0.001)

# 创建 RecDB 实例
rec_db = recdb.RecDB(18)

# 调用 RecDB 的方法
rec_db.Open("/mnt/nvme0n1/gm/ev-table-all.kaggledb18")

# 生成输入数据
# gen_inputs(26, 32)
gen_inputs_fromfile()

# 训练模型
train_model(model, dataloader, criterion, optimizer, num_epochs=10)
