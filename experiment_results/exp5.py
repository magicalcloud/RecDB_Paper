import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

df = pd.read_csv("exp5.csv")

sns.set(style="whitegrid", font="serif")

colors = df["Optimazations"].apply(
    lambda x: "#a8d08d" if "RocksDB" in x else "#c5b4e3"
)

plt.figure(figsize=(12, 5))
bars = plt.barh(
    df["Optimazations"], df["Number of loaded data blocks(*10^5)"],
    color=colors, edgecolor="black", linewidth=0.6
)

for bar in bars:
    width = bar.get_width()
    plt.text(width + 0.3,                
             bar.get_y() + bar.get_height() / 2,
             f"{width:.2f}",
             va='center', ha='left', fontsize=10)

plt.xlabel("Number of loaded data blocks(*10^5)", fontsize=11)
plt.ylabel("")
plt.grid(axis="x", linestyle="--", linewidth=0.5)
plt.gca().invert_yaxis() 
plt.tight_layout()

plt.show()
