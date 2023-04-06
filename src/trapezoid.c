#include <stdlib.h>
#include <stdio.h>
#include <librdkafka/rdkafka.h>
#include <omp.h>
#include <math.h>

#include "kafka_utils.h"


void estimate_integral(double left, double right, int num_trapezoids, rd_kafka_t *producer, const char *topic);


void benchmark_with_trapezoids(int num_threads, rd_kafka_t *producer, const char *topic)
{
    double left = 0.0;
    double right;
    

#pragma omp parallel
    estimate_integral(left, right, 1000, producer, topic);

    
}

// This sends once per thread, so should probably calculate multiple integrals in each test run
void estimate_integral(double left, double right, int num_trapezoids, rd_kafka_t *producer, const char *topic)
{
    
    double trapezoid_base, x, my_result;
    double local_left, local_right;
    int trapezoids_per_thread;

    int my_rank = omp_get_thread_num();
    int num_threads = omp_get_num_threads();

    trapezoid_base = (right-left)/num_trapezoids;
    trapezoids_per_thread = num_trapezoids/num_threads;
    local_left = left + my_rank*(trapezoid_base*trapezoids_per_thread);
    local_right = local_left + trapezoid_base*trapezoids_per_thread;

    my_result = (sin(local_left) + sin(local_right))/2.0;
    for (int i = 1; i < trapezoids_per_thread; i++)
    {
        x = local_left + i * trapezoid_base;
        my_result += sin(x);
    }
    
    // After each thread has calculated the estimate for their part of the integral
    // send the result to the broker
    send_message(producer, topic, &my_result, sizeof(my_result));
}