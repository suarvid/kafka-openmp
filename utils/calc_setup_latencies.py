import sys
import os
import json

broker_name = "192.168.0.131:9092/0"

target_dir = sys.argv[1]
producer_config = sys.argv[2]
num_cores = sys.argv[3]

if producer_config == "shared":
    num_producers = 1
else:
    num_producers = int(num_cores)

file_path_pattern = ""

if len(sys.argv) == 6:
    areas_per_msg = int(sys.argv[5])
    file_path_pattern = "_%s_cores_%s_%s" % (num_cores, producer_config, areas_per_msg)
else:
    file_path_pattern = "_%s_cores_%s" % (num_cores, producer_config)

print("Current file path pattern: %s" % file_path_pattern)
print("With %d producers" % num_producers)

file_count = 0
avg_int_latency_sum = 0
avg_outbuf_latency_sum = 0
txidle_sum = 0

file_paths = [f for f in os.listdir(target_dir) if os.path.isfile(os.path.join(target_dir, f))]
for path in file_paths:
    if file_path_pattern in path:
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