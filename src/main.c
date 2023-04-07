#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <sys/resource.h>

#include <librdkafka/rdkafka.h>
#include "read_data.h"
#include "parallel_producer.h"
#include "producer_builder.h"
#include "kafka_utils.h"
#include <omp.h>

#define MEASUREMENTS_PER_RUN 5
#define SLEEP_BETWEEN_MEASUREMENTS 20 //seconds

void write_summary_stats(FILE *stats_fp, int cores, double elapsed_avg, size_t file_size);

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

// Should create one of each kind of producer
// Is kind of ass, but unlikely to change
producer_info_t *init_producers(const char *brokers)
{
    producer_info_t *producer_infos = malloc(sizeof(producer_info_t) * NUM_PRODUCER_TYPES);
    producer_infos[0].producer = create_producer_basic(brokers);
    producer_infos[0].producer_name = "basic_producer";
    producer_infos[1].producer = create_producer_ack_one(brokers);
    producer_infos[1].producer_name = "producer_ack_one";
    producer_infos[2].producer = create_producer_high_throughput_all_acks_idemp_enabled_gzip(brokers);
    producer_infos[2].producer_name = "producer_high_throughput_all_acks_idemp_enabled_gzip";
    producer_infos[3].producer = create_producer_high_throughput_all_acks_idemp_enabled_snappy(brokers);
    producer_infos[3].producer_name = "producer_high_throughput_all_acks_idemp_enabled_snappy";
    producer_infos[4].producer = create_producer_high_throughput_all_acks_idemp_enabled_lz4(brokers);
    producer_infos[4].producer_name = "producer_high_throughput_all_acks_idemp_enabled_lz4";
    producer_infos[5].producer = create_producer_high_throughput_all_acks_no_idemp_gzip(brokers);
    producer_infos[5].producer_name = "producer_high_throughput_all_acks_no_idemp_gzip";
    producer_infos[6].producer = create_producer_high_throughput_all_acks_no_idemp_snappy(brokers);
    producer_infos[6].producer_name = "producer_high_throughput_all_acks_no_idemp_snappy";
    producer_infos[7].producer = create_producer_high_throughput_all_acks_no_idemp_lz4(brokers);
    producer_infos[7].producer_name = "producer_high_throughput_all_acks_no_idemp_lz4";
    producer_infos[8].producer = create_producer_high_throughput_no_acks_no_idemp_gzip(brokers);
    producer_infos[8].producer_name = "producer_high_throughput_no_acks_no_idemp_gzip";
    producer_infos[9].producer = create_producer_high_throughput_no_acks_no_idemp_lz4(brokers);
    producer_infos[9].producer_name = "producer_high_throughput_no_acks_no_idemp_lz4";
    producer_infos[10].producer = create_producer_high_throughput_no_acks_no_idemp_snappy(brokers);
    producer_infos[10].producer_name = "producer_high_throughput_no_acks_no_idemp_snappy";
    // Should maybe try to do the same without compression?
    return producer_infos;
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

    char *stats_fp_base = "output_data/";
    char actual_cores_str[3];
    sprintf(actual_cores_str, "%d", actual_cores);
    fprintf(stderr, "Running with %d cores\n", actual_cores);

    FILE *input_fp = fopen(input_file, "rb");
    if (input_fp == NULL)
    {
        fprintf(stderr, "Failed to open input file %s\n", input_file);
    }

    FILE *stats_fp;
    size_t input_file_size = get_file_size(input_fp);
    char *input_file_buffer = malloc(input_file_size);
    read_file_contents(input_fp, input_file_buffer, input_file_size);
    fprintf(stderr, "Read file contents\n");

    producer_info_t *producer_infos = init_producers(brokers);
    producer_info_t curr_producer_info;
    fprintf(stderr, "Initiated producers\n");

    char curr_stats_fp_path[512];
    double wtime_start;
    double wtime_end;
    double wtime_elapsed;
    double wtime_elapsed_total = 0.0;
    double wtime_elapsed_avg;

    // Run once per type of producer
    for (int i = 0; i < NUM_PRODUCER_TYPES; i++)
    {
        curr_producer_info = producer_infos[i];
        // Shared
        sprintf(curr_stats_fp_path, "%s%s_%s_cores_shared.json", stats_fp_base, curr_producer_info.producer_name, actual_cores_str);
        stats_fp = init_stats_fp(curr_stats_fp_path);

        fprintf(stderr, "Running with producer %s\n", curr_producer_info.producer_name);
        fprintf(stderr, "Logging stats to file %s\n", curr_stats_fp_path);

        for (int measurement = 0; measurement < MEASUREMENTS_PER_RUN; measurement++)
        {
            wtime_start = omp_get_wtime();
            publish_parallel_for_shared(input_file_size, input_file_buffer, curr_producer_info.producer, topic, actual_cores);
            wtime_end = omp_get_wtime();
            wtime_elapsed = wtime_end - wtime_start;
            wtime_elapsed_total += wtime_elapsed;
            sleep(SLEEP_BETWEEN_MEASUREMENTS);
        }

        wtime_elapsed_avg = wtime_elapsed_total / MEASUREMENTS_PER_RUN;

        write_summary_stats(stats_fp, actual_cores, wtime_elapsed_avg, input_file_size);
        fprintf(stderr, "{ \"n_cores\": %d, \"elapsed_time_avg\": %f, \"bytes_sent\": %zu }\n", actual_cores, wtime_elapsed_avg, input_file_size);
        fclose(stats_fp);
    }

    free(producer_infos);

    producer_info_t **private_producer_infos = malloc(sizeof(struct producer_info *));
    for (int thread_num = 0; thread_num < actual_cores; thread_num++)
    {
        private_producer_infos[thread_num] = init_producers(brokers);
    }

    wtime_elapsed_total = 0.0;

    for (int producer_type = 0; producer_type < NUM_PRODUCER_TYPES; producer_type++)
    {
        curr_producer_info = private_producer_infos[0][producer_type]; // Just use 0, could use any index really
        sprintf(curr_stats_fp_path, "%s%s_%s_cores_private.json", stats_fp_base, curr_producer_info.producer_name, actual_cores_str);
        stats_fp = init_stats_fp(curr_stats_fp_path);

        fprintf(stderr, "Running with producer %s\n", curr_producer_info.producer_name);
        fprintf(stderr, "Logging stats to file %s\n", curr_stats_fp_path);

        for (int measurement = 0; measurement < MEASUREMENTS_PER_RUN; measurement++)
        {
            wtime_start = omp_get_wtime();
            publish_parallel_for_private(input_file_size, input_file_buffer, private_producer_infos, producer_type, topic, actual_cores);
            wtime_end = omp_get_wtime();
            wtime_elapsed = wtime_end - wtime_start;
            wtime_elapsed_total += wtime_elapsed;
            sleep(SLEEP_BETWEEN_MEASUREMENTS);
        }

        wtime_elapsed_avg = wtime_elapsed_total / MEASUREMENTS_PER_RUN;
        write_summary_stats(stats_fp, actual_cores, wtime_elapsed_avg, input_file_size);
        fprintf(stderr, "{ \"n_cores\": %d, \"elapsed_time_avg\": %f, \"bytes_sent\": %zu }\n", actual_cores, wtime_elapsed_avg, input_file_size);

        fclose(stats_fp);
    }

    return EXIT_SUCCESS;
}

void write_summary_stats(FILE *stats_fp, int cores, double elapsed_avg, size_t file_size)
{
    fprintf(stats_fp, "{ \"n_cores\": %d, \"elapsed_time_avg\": %f, \"bytes_sent\": %zu }\n", cores, elapsed_avg, file_size);
    fflush(stats_fp);
}