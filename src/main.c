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
#include "trapezoid.h"
#include <omp.h>

void write_summary_stats(FILE *stats_fp, int cores, double elapsed_avg, size_t file_size);
void benchmark_binary_data(FILE *input_fp, char *brokers, char *topic, char *stats_fp_base, int actual_cores);
void benchmark_with_trapezoids(int n_threads, unsigned long long n_trapezoids, char *topic, char *brokers, char *stats_fp_base);
producer_info_t **init_private_producer_infos(int actual_cores, char *brokers);
int get_actual_n_cores(int n_requested_cores);

int main(int argc, char **argv)
{
    get_elaped_total_sum();
    //const char *brokers;
    //const char *topic;
    //const char *input_file;
    //int n_requested_cores;
    //int actual_cores;

    //if (argc != 5)
    //{
    //    fprintf(stderr, "%% Usage: %s <broker> <topic> <inputfile> <n_cores> \n", argv[0]);
    //    return EXIT_FAILURE;
    //}

    //brokers = argv[1];
    //topic = argv[2];
    //input_file = argv[3];
    //n_requested_cores = atoi(argv[4]);
    //actual_cores = get_actual_n_cores(n_requested_cores);

    //char *stats_fp_base_binary = "output_data/binary/";
    //char *stats_fp_base_trap = "output_data/trapezoids/";
    //char actual_cores_str[3];
    //sprintf(actual_cores_str, "%d", actual_cores);
    //fprintf(stderr, "Running with %d cores\n", actual_cores);

    //FILE *input_fp = fopen(input_file, "rb");
    //if (input_fp == NULL)
    //{
    //    fprintf(stderr, "Failed to open input file %s\n", input_file);
    //}

    ////benchmark_binary_data(input_fp, brokers, topic, stats_fp_base_binary, actual_cores);
    //benchmark_with_trapezoids(actual_cores, 1000000000, topic, brokers, stats_fp_base_trap);

    return EXIT_SUCCESS;
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


void benchmark_binary_data(FILE *input_fp, char *brokers, char *topic, char *stats_fp_base, int actual_cores)
{

    FILE *stats_fp;
    size_t input_file_size = get_file_size(input_fp);
    char *input_file_buffer = malloc(input_file_size);
    read_file_contents(input_fp, input_file_buffer, input_file_size);
    fprintf(stderr, "Read file contents\n");

    //producer_info_t *producer_infos = init_producers(brokers);
    //producer_info_t *producer_infos = init_producers_reverse_order(brokers);
    producer_info_t *producer_infos = init_new_producer_types(brokers);
    producer_info_t curr_producer_info;
    fprintf(stderr, "Initiated producers\n");

    char curr_stats_fp_path[512];
    double wtime_start;
    double wtime_end;
    double wtime_elapsed;
    double wtime_elapsed_total = 0.0;
    double wtime_elapsed_avg;

    // Run once per type of producer
    for (int i = 0; i < NUM_NEW_PRODUCER_TYPES; i++)
    {
        curr_producer_info = producer_infos[i];
        // Shared
        sprintf(curr_stats_fp_path, "%s%s_%d_cores_shared.json", stats_fp_base, curr_producer_info.producer_name, actual_cores);
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

    producer_info_t **private_producer_infos = init_private_producer_infos(actual_cores, brokers);
    //producer_info_t **private_producer_infos = init_new_private_producer_infos(actual_cores, brokers);

    //wtime_elapsed_total = 0.0;

    //for (int producer_type = 0; producer_type < NUM_NEW_PRODUCER_TYPES; producer_type++)
    //{
    //    curr_producer_info = private_producer_infos[0][producer_type]; // Just use 0, could use any index really
    //    sprintf(curr_stats_fp_path, "%s%s_%d_cores_private.json", stats_fp_base, curr_producer_info.producer_name, actual_cores);
    //    stats_fp = init_stats_fp(curr_stats_fp_path);

    //    fprintf(stderr, "Running with producer %s\n", curr_producer_info.producer_name);
    //    fprintf(stderr, "Logging stats to file %s\n", curr_stats_fp_path);

    //    for (int measurement = 0; measurement < MEASUREMENTS_PER_RUN; measurement++)
    //    {
    //        wtime_start = omp_get_wtime();
    //        publish_parallel_for_private(input_file_size, input_file_buffer, private_producer_infos, producer_type, topic, actual_cores);
    //        wtime_end = omp_get_wtime();
    //        wtime_elapsed = wtime_end - wtime_start;
    //        wtime_elapsed_total += wtime_elapsed;
    //        sleep(SLEEP_BETWEEN_MEASUREMENTS);
    //    }

    //    wtime_elapsed_avg = wtime_elapsed_total / MEASUREMENTS_PER_RUN;
    //    write_summary_stats(stats_fp, actual_cores, wtime_elapsed_avg, input_file_size);
    //    fprintf(stderr, "{ \"n_cores\": %d, \"elapsed_time_avg\": %f, \"bytes_sent\": %zu }\n", actual_cores, wtime_elapsed_avg, input_file_size);

    //    // TODO: Maybe free the used producer info here? Clear some RAM
    //    fclose(stats_fp);
    //    for (int core = 0; core < actual_cores; core++)
    //    {
    //        rd_kafka_destroy(private_producer_infos[core][producer_type].producer);
    //    }
    //}
}

void benchmark_with_trapezoids(int n_threads, unsigned long long n_trapezoids, char *topic, char *brokers, char *stats_fp_base)
{
    producer_info_t **private_prod_infos = init_private_producer_infos(n_threads, brokers);
    benchmark_with_trapezoids_private(n_threads, n_trapezoids, topic, private_prod_infos);
    benchmark_with_trapezoids_shared(n_threads, n_trapezoids, topic, brokers);
}


producer_info_t **init_private_producer_infos(int actual_cores, char *brokers)
{
    producer_info_t **private_producer_infos = malloc(sizeof(struct producer_info *) * actual_cores);
    for (int thread_num = 0; thread_num < actual_cores; thread_num++)
    {
        private_producer_infos[thread_num] = init_producers(brokers);
    }

    return private_producer_infos;
}