#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <sys/resource.h>

#include <librdkafka/rdkafka.h>
#include "read_data.h"
#include "parallel_producer.h"
#include "kafka_utils.h"
#include <omp.h>

#define MEASUREMENTS_PER_RUN 5

static void callback(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque)
{
    if (rkmessage->err)
    {
        fprintf(stderr, "%% Message delivery failed: %s\n", rd_kafka_err2str(rkmessage->err));
    }
    // rkmessage destroyed automagically by kafka
}

int get_actual_n_cores(int n_requested_cores)
{
    int max_cores = omp_get_max_threads();
    if (n_requested_cores > max_cores)
    {
        fprintf(stderr, "Number of cores requested is greater than the number of cores available. Using %d cores instead.\n", max_cores);
        return max_cores;
    }
    return n_requested_cores;
}

int main(int argc, char **argv)
{
    const char *brokers;
    const char *topic;
    const char *input_file;
    int n_requested_cores;
    int actual_cores;

    if (argc != 5)
    {
        fprintf(stderr, "%% Usage: %s <broker> <topic> <inputfile> <n_cores> \n", argv[0]);
        return EXIT_FAILURE;
    }

    brokers = argv[1];
    topic = argv[2];
    input_file = argv[3];
    n_requested_cores = atoi(argv[4]);
    actual_cores = get_actual_n_cores(n_requested_cores);
    fprintf(stderr, "Running with %d cores\n", actual_cores);

    FILE *fp = fopen(input_file, "rb");

    if (fp == NULL)
    {
        fprintf(stderr, "Failed to open file!\n");
        exit(EXIT_FAILURE);
    }

    double wtime_start;
    double wtime_end;
    double wtime_elapsed;

    clock_t cpu_time_start;
    clock_t cpu_time_end;
    clock_t cpu_time_elapsed;
    double cpu_utilization;

    size_t bytes_sent;
    FILE *stats_fp;

    stats_fp = init_stats_fp("output_data/stats_shared.json");
    fprintf(stderr, "Running shared producers...\n");
    for (int i = 0; i < MEASUREMENTS_PER_RUN; i++)
    {
        wtime_start = omp_get_wtime();
        cpu_time_start = clock();
        bytes_sent = publish_with_omp_shared_producer(fp, brokers, topic, n_requested_cores);
        wtime_end = omp_get_wtime();
        cpu_time_end = clock();
        wtime_elapsed = wtime_end - wtime_start;
        cpu_time_elapsed = ((double)cpu_time_end - cpu_time_start) / CLOCKS_PER_SEC;
        cpu_utilization = cpu_time_elapsed / wtime_elapsed;
        cpu_utilization /= actual_cores;
        fprintf(stats_fp, "{ \"n_cores\": %d, \"cpu_utilization\": %f, \"seconds_elapsed\": %f, \"bytes_sent\": %zu }\n", actual_cores, cpu_utilization, wtime_elapsed, bytes_sent);
        sleep(10);
    }

    stats_fp = init_stats_fp("output_data/stats_private.json");
    fprintf(stderr, "Running private producers...\n");
    for (int i = 0; i < MEASUREMENTS_PER_RUN; i++)
    {
        wtime_start = omp_get_wtime();
        cpu_time_start = clock();
        bytes_sent = publish_with_omp_private_producer(fp, brokers, topic, n_requested_cores);
        wtime_end = omp_get_wtime();
        cpu_time_end = clock();
        wtime_elapsed = wtime_end - wtime_start;
        cpu_time_elapsed = ((double)cpu_time_end - cpu_time_start) / CLOCKS_PER_SEC;
        cpu_utilization = cpu_time_elapsed / wtime_elapsed;
        cpu_utilization /= actual_cores;
        fprintf(stats_fp, "{ \"n_cores\": %d, \"cpu_utilization\": %f, \"seconds_elapsed\": %f, \"bytes_sent\": %zu }\n", actual_cores, cpu_utilization, wtime_elapsed, bytes_sent);
        sleep(10); // just to give some room for each run
    }

    fclose(fp);
    fclose(stats_fp);

    return 0;
}