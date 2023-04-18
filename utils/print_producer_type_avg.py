import sys
import os
import json

target_dir = sys.argv[1]
producer_name = sys.argv[2]

print("Current producer name: %s" % producer_name)


file_paths = [f for f in os.listdir(target_dir) if os.path.isfile(os.path.join(target_dir, f))]
for path in file_paths:
    if producer_name in path:
        with open(os.path.join(target_dir, path)) as f:
            for line in f:
                pass
            last_line = line
            try:
                last_line_dict = json.loads(last_line)
            except:
                print("Error parsing line: %s" % last_line)
                continue
            elapsed_time_avg = last_line_dict["elapsed_time_avg"]
            print("File: %s, Elapsed time avg: %s" % (path, elapsed_time_avg))
