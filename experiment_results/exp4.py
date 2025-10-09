import matplotlib.pyplot as plt
import pandas as pd


# data = {
#     'RecDB_wokeyagent': [1, 1, 2, 2, 3, 3, None, None, None, None, None, None, None, None],
#     'RecDB_wokeyagent_woreadscheduler': [1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7],
#     'CDF_RocksDB': [0, 0.99925, 0.99925, 1, 1, 1, None, None, None, None, None, None, None, None],
#     'CDF_RP(KS)': [0, 0.905, 0.905, 0.996, 0.996, 0.9996, 0.9996, 0.99996, 0.99996, 1, 1, 1, 1, 1]
# }
df = pd.read_csv("exp4.csv")


df_rocksdb = df[['RocksDB', 'CDF_RocksDB']].dropna().sort_values(by='RocksDB')
df_rpks = df[['RecDB_wokeyagent', 'CDF_RP(KS)']].dropna().sort_values(by='RecDB_wokeyagent')


fig, ax1 = plt.subplots(figsize=(10, 6))


ax1.step(df_rocksdb['RocksDB'], df_rocksdb['CDF_RocksDB'], where='post', color='#a8d08d', linewidth=2, label='RocksDB')


ax1.step(df_rpks['RecDB_wokeyagent'], df_rpks['CDF_RP(KS)'], where='post', color='red', linewidth=2, label='RP(KS)')


ax1.set_xlabel('Number of reloads', fontsize=16)
ax1.set_ylabel('CDF', fontsize=16)
ax1.set_xlim(0, 16)
ax1.set_ylim(0.7, 1.0)  
ax1.set_xticks(range(0, 17, 2))
ax1.tick_params(axis='both', which='major', labelsize=14)
ax1.grid(True, linestyle='--', alpha=0.7)
ax1.legend(fontsize=14, loc='lower left', bbox_to_anchor=(0, 1.02, 1, 0),
           ncol=2, mode='expand', frameon=False)

plt.tight_layout()
plt.show()