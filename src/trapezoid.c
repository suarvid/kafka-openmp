#include <stdlib.h>
#include <stdio.h>
#include <librdkafka/rdkafka.h>
#include <omp.h>
#include <math.h>
#include <unistd.h>

#include "kafka_utils.h"
#include "trapezoid.h"
#include "producer_builder.h"

#define RIGHT_POINT_TRAPEZOID 1000.0
#define AREAS_PER_MSG 100

void estimate_integral(double left, double right, unsigned long long num_trapezoids, int areas_per_msg, int n_threads, int thread_id, rd_kafka_t *producer, const char *topic);

void benchmark_with_trapezoids_private(int n_threads, unsigned long long n_trapezoids, int areas_per_msg, const char *topic, producer_info_t **private_producer_infos)
{
    fprintf(stderr, "Benchmarking with trapezoids (private) using %d threads, %d areas per msg\n", n_threads, areas_per_msg);
    double left = 0.0;
    double wtime_start;
    double wtime_end;
    double wtime_elapsed_total = 0.0;
    double wtime_elapsed_avg = 0.0;
    char curr_stats_fp_path[512];
    FILE *stats_fp;
    char *stats_fp_base = "output_data/trapezoids/";

    for (int producer_type = 0; producer_type < FINAL_NUM_PRODUCER_TYPES; producer_type++)
    {
        char *test_prod_name = private_producer_infos[0][0].producer_name;
        char *curr_producer_name = private_producer_infos[0][producer_type].producer_name;
        sprintf(curr_stats_fp_path, "%s%s_%d_cores_private_%d_areas_per_message.json", stats_fp_base, curr_producer_name, n_threads, areas_per_msg);
        stats_fp = init_stats_fp(curr_stats_fp_path);
        for (int measurement = 0; measurement < MEASUREMENTS_PER_RUN; measurement++)
        {
            wtime_start = omp_get_wtime();
#pragma omp parallel num_threads(n_threads)
            {
                int thread_id = omp_get_thread_num();
                producer_info_t curr_prod_info = private_producer_infos[thread_id][producer_type];
                estimate_integral(left, RIGHT_POINT_TRAPEZOID, n_trapezoids, areas_per_msg, n_threads, thread_id, curr_prod_info.producer, topic);
            }
            wtime_end = omp_get_wtime();
            wtime_elapsed_total += (wtime_end - wtime_start);
            sleep(SLEEP_BETWEEN_MEASUREMENTS);
        }
        wtime_elapsed_avg = wtime_elapsed_total / MEASUREMENTS_PER_RUN;
        write_summary_stats(stats_fp, n_threads, wtime_elapsed_avg, 0);
    }
}

void benchmark_with_trapezoids_shared(int n_threads, unsigned long long n_trapezoids, int areas_per_msg, const char *topic, const char *brokers)
{
    fprintf(stderr, "Benchmarking with trapezoids (shared) using %d threads, %d areas per msg\n", n_threads, areas_per_msg);
    double left = 0.0;
    double wtime_start;
    double wtime_end;
    double wtime_elapsed_total = 0.0;
    double wtime_elapsed_avg = 0.0;
    char curr_stats_fp_path[512];
    FILE *stats_fp;
    char *stats_fp_base = "output_data/trapezoids/";

    producer_info_t *producer_infos = init_final_producer_types(brokers);

    for (int producer_type = 0; producer_type < FINAL_NUM_PRODUCER_TYPES; producer_type++)
    {
        char *curr_producer_name = producer_infos[producer_type].producer_name;
        sprintf(curr_stats_fp_path, "%s%s_%d_cores_shared_%d_areas_per_message.json", stats_fp_base, curr_producer_name, n_threads, areas_per_msg);
        stats_fp = init_stats_fp(curr_stats_fp_path);
        for (int measurement = 0; measurement < MEASUREMENTS_PER_RUN; measurement++)
        {
            rd_kafka_t *curr_producer = producer_infos[producer_type].producer;
            wtime_start = omp_get_wtime();
#pragma omp parallel num_threads(n_threads)
            {

                int thread_id = omp_get_thread_num();
                estimate_integral(left, RIGHT_POINT_TRAPEZOID, n_trapezoids, areas_per_msg, n_threads, thread_id, curr_producer, topic);
            }
            wtime_end = omp_get_wtime();
            wtime_elapsed_total += (wtime_end - wtime_start);
            sleep(SLEEP_BETWEEN_MEASUREMENTS);
        }
        wtime_elapsed_avg = wtime_elapsed_total / MEASUREMENTS_PER_RUN;
        write_summary_stats(stats_fp, n_threads, wtime_elapsed_avg, 0);
    }
}

// This sends once per thread, so should probably calculate multiple integrals in each test run
void estimate_integral(double left, double right, unsigned long long num_trapezoids, int areas_per_mgs, int n_threads, int thread_id, rd_kafka_t *producer, const char *topic)
{

    double trapezoid_base, x, res;
    double local_left, local_right;
    long area_cnt = 0;
    unsigned long long trapezoids_per_thread;

    trapezoid_base = (right - left) / num_trapezoids;
    trapezoids_per_thread = num_trapezoids / n_threads;
    if (thread_id == 0)
    {
        fprintf(stderr, "Trapezoids per thread: %ld\n", trapezoids_per_thread);
    }
    local_left = left + thread_id * (trapezoid_base * trapezoids_per_thread);
    local_right = local_left + trapezoid_base * trapezoids_per_thread;

    res += (sin(local_left) + sin(local_right)) / 2.0;
    for (int i = 1; i < trapezoids_per_thread; i++)
    {
        x = local_left + i * trapezoid_base;
        res += sin(x);
        area_cnt++;
        if (area_cnt == AREAS_PER_MSG) // Decides the rate at which messages should be sent
        {
            //fprintf(stderr, "Sending message\n");
            send_message(producer, topic, (char *)&res, sizeof(res));
            area_cnt = 0;
        }
        // my_result += sin(x);
    }
}

double area_function(double x)
{
    return 3.1415 * cos(x) * sin(x);
}