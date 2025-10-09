import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from io import StringIO

df = pd.read_csv("exp3_a.csv")


sns.set(style="whitegrid", font="serif")


colors = df["Optimization"].apply(lambda x: "#c5b4e3" if "RP" in x else "#a8d08d")


plt.figure(figsize=(5, 3))
bars = plt.bar(df["Optimization"], df["prefetch latency"], color=colors, edgecolor="black", linewidth=0.6)


plt.ylabel("Read Latency", fontsize=11)
plt.xlabel("")
plt.grid(axis="y", linestyle="--", linewidth=0.5)
plt.tight_layout()


plt.show()



