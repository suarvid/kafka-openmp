import json
import sys

broker_name = "192.168.0.131:9092/0"

input_file_path = sys.argv[1]
handle = open(input_file_path, 'r')
lines = handle.readlines()

line_count = 0
int_latency_sum = 0
outbuf_latency_sum = 0
int_latency_max = []
outbuf_latency_max = []
int_latency_min = []
outbuf_latency_min = []
outbuf_latency_stddev_sum = 0.0
int_latency_stddev_sum = 0.0
cpu_utilization_sum = 0.0
seconds_elapsed = 0.0
bytes_sent = 0
n_cores = 0

for line in lines:
    line_values = json.loads(line)
    if broker_name in line:
        int_latency_sum += line_values["brokers"][broker_name]["int_latency"]["avg"]
        outbuf_latency_sum += line_values["brokers"][broker_name]["outbuf_latency"]["avg"]
        int_latency_max.append(line_values["brokers"][broker_name]["int_latency"]["max"])
        outbuf_latency_max.append(line_values["brokers"][broker_name]["outbuf_latency"]["max"])
        int_latency_min.append(line_values["brokers"][broker_name]["int_latency"]["min"])
        outbuf_latency_min.append(line_values["brokers"][broker_name]["outbuf_latency"]["min"])
        int_latency_stddev_sum += line_values["brokers"][broker_name]["int_latency"]["stddev"]
        outbuf_latency_stddev_sum += line_values["brokers"][broker_name]["outbuf_latency"]["stddev"]

    line_count += 1

int_latency_avg = int_latency_sum / line_count
outbuf_latency_avg = outbuf_latency_sum / line_count
total_latency_avg = int_latency_avg + outbuf_latency_avg
int_latency_min = min(int_latency_min)
int_latency_max = max(int_latency_max)
outbuf_latency_min = min(outbuf_latency_min)
outbuf_latency_max = max(outbuf_latency_max)
int_latency_stddev = int_latency_stddev_sum / line_count
outbuf_latency_stddev = outbuf_latency_stddev_sum / line_count
average_throughput = bytes_sent / seconds_elapsed
cpu_utilization_avg = cpu_utilization_sum / line_count

output_lines = []
output_lines.append("Average total latency: {} (ms)\n".format(total_latency_avg / 1000))
output_lines.append("Maximum internal latency: {} (ms)\n".format(int_latency_max / 1000))
output_lines.append("Minimum internal latency: {} (ms)\n".format(int_latency_min / 1000))
output_lines.append("Maximum outbuf latency: {} (ms)\n".format(outbuf_latency_max / 1000))
output_lines.append("Minimum outbuf latency: {} (ms)\n".format(outbuf_latency_min / 1000))
output_lines.append("Internal latency stddev: {} (ms)\n".format(int_latency_stddev / 1000))
output_lines.append("Outbuf latency stddev: {} (ms)\n".format(outbuf_latency_stddev / 1000))

output_file = open(input_file_path, "a")

output_file.writelines(output_lines)
