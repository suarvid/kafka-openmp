# A script which calculates the runtime of the specified profiles
# for the specified number of threads and specified producer config (shared/private),
# in the specified directory.
# Outputs the runtimes to stdout.

import os
import sys
import json

# The first profile name to process
first_profile = sys.argv[1]

# The second profile name to process
second_profile = sys.argv[2]

# The number of threads to process
num_threads = int(sys.argv[3])

# The producer config to process
producer_config = sys.argv[4]

# The directory to process
target_dir = sys.argv[5]

def matches_pattern(file_name, profile, num_threads, producer_config) -> bool:
    return profile in file_name and str(num_threads) in file_name and producer_config in file_name


def get_profile_runtime(profile) -> float:
    file_paths = [f for f in os.listdir(target_dir) if os.path.isfile(os.path.join(target_dir, f))]

    for path in file_paths:
        if matches_pattern(path, profile, num_threads, producer_config):
            first_profile_path = os.path.join(target_dir, path)
            print("Found first profile file: %s" % first_profile_path)
            with open(os.path.join(target_dir, path)) as f:
                for line in f:
                    continue
                last_line = line
                first_profile_values = json.loads(last_line)
                return first_profile_values["elapsed_time_avg"]


first_runtime = get_profile_runtime(first_profile)
second_runtime = get_profile_runtime(second_profile)

print("First Profile: %s, runtime: %s" % (first_profile, first_runtime))
print("Second Profile: %s, runtime: %s" % (second_profile, second_runtime))