import json
import os
import matplotlib.pyplot as plt

# Plot style configuration
def plot_series(x, y_series, labels, title, y_label):
    plt.figure(figsize=(10, 5))
    ax = plt.gca()
    colors = ['#99d8c9', '#fdbb84', '#fc9272']
    fill_colors = ['#ccece6', '#fee8c8', '#fdd0a2']

    for i, y in enumerate(y_series):
        plt.plot(x, y, label=labels[i], color=colors[i], linewidth=3.5)
        plt.fill_between(x, y, alpha=0.1, color='#333333')

    # plt.xscale('log')
    plt.grid(True, which="both", linestyle='--', linewidth=0.5, alpha=0.4)
    plt.xlabel("Range Size")
    plt.ylabel(y_label)
    plt.title(title)
    plt.legend()
    ax.tick_params()
    plt.tight_layout()
    plt.show()

# Directory containing benchmark JSON files
json_dir = os.path.dirname(os.path.realpath(__file__))
json_files = [f for f in os.listdir(json_dir) if f.endswith(".json")]


# Temporary store for parsed results
results = []

# Parse each file into structured results
for filename in json_files:
    range_size = int(filename.split(".")[0])
    with open(os.path.join(json_dir, filename)) as f:
        data = json.load(f)
        results.append((range_size, data))
        
# Sort all entries by range size
results.sort(key=lambda x: x[0])

# Separate into individual lists after sorting
range_sizes = []
lat_p50, lat_p90, lat_p99 = [], [], []
blk_p50, blk_p90, blk_p99 = [], [], []

for size, data in results:
    range_sizes.append(size)

    lat = data["latencies_ns"]
    lat_p50.append(lat["p50"] / 1000000)
    lat_p90.append(lat["p90"] / 1000000)
    lat_p99.append(lat["p99"] / 1000000)

    blk = data["blocks_read"]
    blk_p50.append(blk["p50"])
    blk_p90.append(blk["p90"])
    blk_p99.append(blk["p99"])

# Plot latency
plot_series(
    range_sizes,
    [lat_p50, lat_p90, lat_p99],
    ["p50", "p90", "p99"],
    "Latency vs Range Size",
    "Latency (ms)"
)

# Plot blocks read
plot_series(
    range_sizes,
    [blk_p50],
    ["p50"],
    "Blocks Read vs Range Size",
    "Blocks Read"
)