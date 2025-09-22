import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

def generate_comparison_graphs_with_stats():
    # --- Data Ingestion ---
    parameters = [
        "1M rows, 100 files, 100 cols", "1M rows, 10 files, 100 cols", "1M rows, 1 file, 100 cols",
        "1M rows, 1 file, 200 cols", "1M rows, 1 file, 300 cols", "5M rows, 100 files, 100 cols",
        "5M rows, 10 files, 100 cols", "5M rows, 1 file, 100 cols", "5M rows, 1 file, 200 cols",
        "5M rows, 1 file, 300 cols", "10M rows, 100 files, 100 cols", "10M rows, 10 files, 100 cols",
        "10M rows, 1 file, 100 cols", "100M rows, 100 files, 10 cols", "100M rows, 10 files, 10 cols",
        "100M rows, 1 file, 10 cols", "200M rows, 1 file, 10 cols"
    ]

    actor3_v1 = [30.74, 11.23, 21.25, 38.26, 57.07, 62.54, 38.78, 84.55, 194.42, 296.81, 115.36, 75.03, 174.61, 51.43, 47.76, 100.38, 379.21]
    actor3_v2 = [23.93, 8.92, 9.45, 15.34, 21.78, 53.83, 34.23, 30.35, 60.42, 100.99, 75.77, 59.07, 56.21, 45.566, 64.45, 41.88, 78.11]
    actor3_v4 = [10.16, 8.56, 8.28, 14.76, 22.30, 32.47, 31.56, 32.18, 58.20, 97.82, 54.68, 52.09, 59.70, 32.03, 40.77, 41.99, 66.45]
    spark = [21.39, 20.81, 19.32, 24.71, 27.56, 29.01, 30.02, 30.22, 41.54, 89.02, 40.20, 38.34, 37.54, 23.76, 35.48, 35.89, 53.35]

    # Create a DataFrame
    df = pd.DataFrame({
        "Test Case": parameters,
        "Actor3 V1 (Sequential)": actor3_v1,
        "Actor3 V2 (Parallel)": actor3_v2,
        "Actor3 V4 (Thread Pool)": actor3_v4,
        "Spark": spark
    })

    # Define the pairs we want to plot
    comparisons = [
        ("Actor3 V1 (Sequential)", "Spark"),
        ("Actor3 V2 (Parallel)", "Spark"),
        ("Actor3 V4 (Thread Pool)", "Spark")
    ]

    # Colors for consistency
    custom_palette = {
        "Actor3 V1 (Sequential)": "#d62728",
        "Actor3 V2 (Parallel)": "#ff7f0e",
        "Actor3 V4 (Thread Pool)": "#2ca02c",
        "Spark": "#1f77b4"
    }

    # Generate plots
    for actor, baseline in comparisons:
        df_subset = df[["Test Case", actor, baseline]]

        # --- Calculate closeness statistics ---
        actor_vals = np.array(df_subset[actor])
        spark_vals = np.array(df_subset[baseline])
        closeness_percent = 100 * np.minimum(actor_vals, spark_vals) / np.maximum(actor_vals, spark_vals)
        avg_closeness = np.mean(closeness_percent)

        # Melt for plotting
        df_melted = df_subset.melt(
            id_vars="Test Case", var_name="Implementation", value_name="Time (s)"
        )

        plt.figure(figsize=(14, 7))
        ax = sns.lineplot(
            data=df_melted, x="Test Case", y="Time (s)", 
            hue="Implementation", palette=custom_palette,
            marker="o", markersize=7, linestyle="-"
        )

        plt.title(
            f"Performance Comparison: {actor} vs {baseline}\n"
            f"Average closeness: {avg_closeness:.2f}%", 
            fontsize=16, pad=20
        )
        plt.xlabel("Test Configuration", fontsize=12, labelpad=15)
        plt.ylabel("Execution Time (seconds)", fontsize=12, labelpad=15)
        plt.xticks(rotation=45, ha="right", fontsize=10)
        plt.yticks(fontsize=10)
        plt.legend(title="Implementation", fontsize=11, title_fontsize=12)
        ax.yaxis.grid(True, linestyle="--", linewidth=0.5)

        # Annotate the average closeness inside the graph
        plt.text(
            0.01, 0.95, f"Avg closeness: {avg_closeness:.2f}%", 
            transform=ax.transAxes, fontsize=12, color="black",
            bbox=dict(facecolor="white", edgecolor="black", boxstyle="round,pad=0.3")
        )

        plt.tight_layout()
        filename = f"performance_{actor.replace(' ', '_').replace('(', '').replace(')', '').replace('/', '_')}_vs_spark.png"
        plt.savefig(filename, dpi=300)
        print(f"Graph saved as '{filename}' with average closeness {avg_closeness:.2f}%")
        plt.show()


if __name__ == "__main__":
    generate_comparison_graphs_with_stats()
