import sys
import os
import json

target_dir = sys.argv[1]
producer_config = sys.argv[2]
num_cores = sys.argv[3]

base_path_pattern = "%s_cores_%s"
current_path_pattern = base_path_pattern % (num_cores, producer_config)

print("Current path pattern: %s" % current_path_pattern)

elapsed_time_avg_sum = 0
file_count = 0

file_paths = [f for f in os.listdir(target_dir) if os.path.isfile(os.path.join(target_dir, f))]
for path in file_paths:
    if current_path_pattern in path:
        with open(os.path.join(target_dir, path)) as f:
            for line in f:
                pass
            last_line = line
            try:
                last_line_dict = json.loads(last_line)
            except:
                print("Error parsing line: %s" % last_line)
                continue
            elapsed_time_avg_sum += last_line_dict["elapsed_time_avg"]
            file_count += 1

elapsed_time_avg_avg = elapsed_time_avg_sum / file_count
print("Elapsed time avg avg: %s" % elapsed_time_avg_avg)