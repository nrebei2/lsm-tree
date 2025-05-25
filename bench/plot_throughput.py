import os
import json
from datetime import datetime
import matplotlib.pyplot as plt

base_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "get")
client_counts = ["1", "2", "4", "8", "16"]
throughputs = []

for folder in client_counts:
    dir_path = os.path.join(base_dir, folder)
    total_requests = 0
    start_times = []
    end_times = []

    for file_name in os.listdir(dir_path):
        if file_name.endswith(".json"):
            with open(os.path.join(dir_path, file_name)) as f:
                data = json.load(f)
                total_requests += data["num_requests"]
                start_times.append(datetime.strptime(data["start_time"], "%H__%M__%S.%f"))
                end_times.append(datetime.strptime(data["end_time"], "%H:%M:%S.%f"))

    earliest_start = min(start_times)
    latest_end = max(end_times)
    duration_seconds = (latest_end - earliest_start).total_seconds()
    throughput = total_requests / duration_seconds if duration_seconds > 0 else 0
    throughputs.append(throughput)

# Plotting
client_nums = list(map(int, client_counts))

plt.figure(figsize=(8, 5))
plt.plot(client_nums, throughputs, marker='o', color='#1f77b4', linewidth=2)
plt.fill_between(client_nums, throughputs, alpha=0.1, color='#333333')

# plt.xscale("log", base=2)
plt.xticks(client_nums, labels=client_counts)
plt.xlabel("Number of Clients")
plt.ylabel("Throughput (GETs per second)")
plt.title("Throughput (Over Internet to PC) vs Number of Clients")
plt.grid(True, linestyle='--', alpha=0.6)
plt.tight_layout()
plt.show()