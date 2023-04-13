#pragma once

#include <librdkafka/rdkafka.h>

void benchmark_with_trapezoids_private(int n_threads, const char *topic);
void benchmark_with_trapezoids_shared(int n_threads, rd_kafka_t *producer, const char *topic);