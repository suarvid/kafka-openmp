import sys
import os
import json

broker_name = "192.168.0.131:9092/0"

target_dir = sys.argv[1]
producer_config = sys.argv[2]
num_cores = sys.argv[3]
num_producers = int(sys.argv[4])

base_path_pattern = "_%s_cores_%s"
current_path_pattern = base_path_pattern % (num_cores, producer_config)

print("Current path pattern: %s" % current_path_pattern)

file_count = 0
avg_int_latency_sum = 0
avg_outbuf_latency_sum = 0
txidle_sum = 0

file_paths = [f for f in os.listdir(target_dir) if os.path.isfile(os.path.join(target_dir, f))]
for path in file_paths:
    if current_path_pattern in path:
        print("Processing file: %s" % path)
        with open(os.path.join(target_dir, path)) as f:
            for line in f:
                if broker_name in line:
                    line_values = json.loads(line)
                    txidle_sum += line_values["brokers"][broker_name]["txidle"]
                    avg_int_latency_sum += line_values["brokers"][broker_name]["int_latency"]["avg"]
                    avg_outbuf_latency_sum += line_values["brokers"][broker_name]["outbuf_latency"]["avg"]


print("Total time between socket sends: {} seconds".format(txidle_sum/(num_producers*1000000))) # Div by 1000000 to get seconds, 1000 to get milliseconds
print("Total internal latency: {} seconds".format(avg_int_latency_sum/(num_producers*1000000)))  
print("Total outbuf latency: {} milliseconds".format(avg_outbuf_latency_sum/(num_producers*1000))) 