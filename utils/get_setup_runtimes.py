import sys
import os
import json

target_dir = sys.argv[1]
producer_config = sys.argv[2]
num_cores = sys.argv[3]

path_pattern = "_%s_cores_%s" % (num_cores, producer_config)

file_paths = [f for f in os.listdir(target_dir) if os.path.isfile(os.path.join(target_dir, f))]

for path in file_paths:
    if path_pattern in path:
        with open(os.path.join(target_dir, path)) as f:
            for line in f:
                pass
            last = line
            try:
                last_dict = json.loads(last)
            except:
                print("Error parsing line: %s" % last)
                continue
            print(f"{path}: {last_dict['elapsed_time_avg']}")