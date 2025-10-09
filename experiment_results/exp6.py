import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

df = pd.read_csv("exp6.csv")

sns.set(style="whitegrid", font="serif")

colors = df["Optimization"].apply(
    lambda x: "#c5b4e3" if "w/" in x else "#a8d08d"
)

plt.figure(figsize=(12, 5))
bars = plt.barh(
    df["Optimization"], df["compaction delete efficiency"],
    color=colors, edgecolor="black", linewidth=0.6
)

for bar in bars:
    width = bar.get_width()
    plt.text(width + 0.3,                
             bar.get_y() + bar.get_height() / 2,
             f"{width:.2f}",
             va='center', ha='left', fontsize=10)

plt.xlabel("GC efficiency (%)", fontsize=11)
plt.ylabel("")
plt.grid(axis="x", linestyle="--", linewidth=0.5)
plt.gca().invert_yaxis() 
plt.tight_layout()

plt.show()
