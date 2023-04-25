#pragma once

#include <librdkafka/rdkafka.h>

void benchmark_with_trapezoids_private(int n_threads, unsigned long long n_trapezoids, int areas_per_msg, const char *topic, producer_info_t **private_producer_infos);
void benchmark_with_trapezoids_shared(int n_threads, unsigned long long n_trapezoids, int areas_per_msg, const char *topic, const char *brokers);