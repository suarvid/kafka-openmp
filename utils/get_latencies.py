import json
import sys

broker_name = "192.168.0.131:9092/0"


input_file_path = sys.argv[1]
num_producer_instances = int(sys.argv[2]) # Each producer instance will contribute to the logs, need to divide by this number
handle = open(input_file_path, 'r')
lines = handle.readlines()

avg_int_latency_sum = 0
avg_outbuf_latency_sum = 0
txidle_sum = 0
elapsed_time_avg = 0

for line in lines:
    line_values = json.loads(line)
    if broker_name in line:
        txidle_sum += line_values["brokers"][broker_name]["txidle"]
        avg_int_latency_sum += line_values["brokers"][broker_name]["int_latency"]["avg"]
        avg_outbuf_latency_sum += line_values["brokers"][broker_name]["outbuf_latency"]["avg"]
    else:
        elapsed_time_avg = line_values["elapsed_time_avg"]
        

print("Total time between socket sends: {} ".format(txidle_sum/num_producer_instances))
print("Total internal latency: {} ".format(avg_int_latency_sum/num_producer_instances))
print("Total outbuf latency: {} ".format(avg_outbuf_latency_sum/num_producer_instances))
print("Average elapsed time: {} ".format(elapsed_time_avg))