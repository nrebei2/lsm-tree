import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec

import os
import json
import numpy as np

def load_data(json_dir):
    data_points = []
    json_files = [f for f in os.listdir(json_dir) if f.endswith(".json")]

    for fname in json_files:
        with open(os.path.join(json_dir, fname)) as f:
            d = json.load(f)
            data_points.append({
                "size_mb": d["database_size"] / 1_000_000,
                "lat_p50": d["latencies_ns"]["p50"] / 1000,
                "lat_p90": d["latencies_ns"]["p90"] / 1000,
                "lat_p99": d["latencies_ns"]["p99"] / 1000,
                "blk_p50": d["blocks_read"]["p50"],
                "blk_p90": d["blocks_read"]["p90"],
                "blk_p99": d["blocks_read"]["p99"],
            })

    data_points.sort(key=lambda x: x["size_mb"])

    return {
        "sizes": [d["size_mb"] for d in data_points],
        "latencies": [
            [d["lat_p50"] for d in data_points],
            [d["lat_p90"] for d in data_points],
            [d["lat_p99"] for d in data_points],
        ],
        "blocks": [
            [d["blk_p50"] for d in data_points],
            [d["blk_p90"] for d in data_points],
            [d["blk_p99"] for d in data_points],
        ],
    }

def plot_series(ax, x, y_series, labels, title, y_label):
    colors = ['#99d8c9', '#fdbb84', '#fc9272']

    if labels is None:
        ax.plot(x, y_series, color=colors[0]) #, linewidth=2.5) #, marker='o')
        ax.fill_between(x, y_series, alpha=0.1, color='#333333')
    else:
        for i, y in enumerate(y_series):
            ax.plot(x[i], y, label=labels[i], color=colors[i], marker='o', markersize=4)
            ax.fill_between(x[i], y, alpha=0.1, color='#333333')
            
    if labels:
        ax.legend()

    ax.set_xlabel("Database size (MB)")
    ax.set_title(title)
    ax.set_ylabel(y_label)
    ax.grid(True, which="both", linestyle='--', linewidth=0.5, alpha=0.4)
    ax.tick_params()
    
def plot_cumulative(ax, x, y_series, labels, y_label, title):
    colors = ['#99d8c9', '#fdbb84', '#fc9272']

    if labels is None:
        cumulative = np.cumsum(y_series)
        ax.plot(x, cumulative, color=colors[0], linewidth=2.5)
        ax.fill_between(x, cumulative, alpha=0.1, color='#333333')
    else:
        for i, y in enumerate(y_series):
            cumulative = np.cumsum(y)
            ax.plot(x[i], cumulative, label=labels[i], color=colors[i], linewidth=2.5)
            ax.fill_between(x[i], cumulative, alpha=0.1, color='#333333')
            
    if labels:
        ax.legend()

    ax.set_xlabel("Database size (MB)")
    ax.set_title(title)
    ax.set_ylabel(y_label)
    ax.grid(True, which="both", linestyle='--', linewidth=0.5, alpha=0.4)
    ax.tick_params()

dir1 = "bench/smaller_block_size/before"
dir2 = "bench/smaller_block_size/after"

data1 = load_data(dir1)
data2 = load_data(dir2)

fig, axes = plt.subplots(2, 1, figsize=(14, 8), sharex=True)

# fig = plt.figure(figsize=(14, 8))
# gs = gridspec.GridSpec(2, 2, height_ratios=[1, 1.5])  # make bottom row taller

# ax_tl = fig.add_subplot(gs[0, 0])
# ax_tr = fig.add_subplot(gs[0, 1])
# ax_btm = fig.add_subplot(gs[1, :])

# Latency plots
plot_series(axes[0], [data1["sizes"], data2["sizes"]], [data1["latencies"][0], data2["latencies"][0]], ["Pre-treatment", "Smaller Block Size"], "Load Latency", "Latency (μs)")
plot_cumulative(axes[1], [data1["sizes"], data2["sizes"]], [data1["latencies"][0], data2["latencies"][0]], ["Pre-treatment", "Smaller Block Size"], "Cumulative Latency (μs)", "Cumulative Loads Latency")

# Blocks read plots
# plot_series(axes[1, 0], data1["sizes"], data1["blocks"], ["p50", "p90", "p99"], "Blocks Read (Dir 1)", "Blocks Read")
# plot_series(axes[1, 1], data2["sizes"], data2["blocks"], ["p50", "p90", "p99"], "Blocks Read (Dir 2)", "Blocks Read")

plt.tight_layout()
plt.show()