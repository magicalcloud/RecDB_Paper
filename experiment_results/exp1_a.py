import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

df = pd.read_csv("exp1_a.csv")

df = df.rename(columns={"Ext-LSM": "RecDB"})

sns.set(style="whitegrid", font="serif")


colors = {
    "HashDB": "#f4b183",  
    "RocksDB": "#a8d08d",  
    "RecDB": "#c5b4e3"     
}

models = df["model"].unique()
fig, axes = plt.subplots(1, len(models), figsize=(10, 3), sharey=True)

for ax, model in zip(axes, models):
    subset = df[df["model"] == model]
    subset_melted = subset.melt(
        id_vars=["batch_size"], 
        value_vars=["HashDB", "RocksDB", "RecDB"],
        var_name="DB", 
        value_name="Speedup"
    )

    sns.barplot(
        data=subset_melted, 
        x="batch_size", 
        y="Speedup", 
        hue="DB",
        palette=colors, 
        ax=ax,
        edgecolor="black", 
        linewidth=0.6
    )

    
    ax.set_title(model, fontsize=11, fontweight="bold", pad=5)
    ax.set_xlabel("")
    ax.set_ylabel("Speedup" if model == models[0] else "")
    ax.legend_.remove()
    ax.grid(axis="y", linestyle="--", linewidth=0.5)
    ax.set_xticklabels(subset["batch_size"].astype(str), rotation=0)

handles, labels = ax.get_legend_handles_labels()
fig.legend(handles, labels, loc="upper center", ncol=3, frameon=True)

plt.tight_layout(rect=[0, 0, 1, 0.9])
plt.show()


