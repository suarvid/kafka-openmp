#pragma once

#include <librdkafka/rdkafka.h>


// Throughput
rd_kafka_t *create_producer_with_config(rd_kafka_conf_t *config);
rd_kafka_t *create_producer_high_throughput_no_acks_no_idemp_snappy(const char *brokers);
rd_kafka_t *create_producer_high_throughput_no_acks_no_idemp_gzip(const char *brokers);
rd_kafka_t *create_producer_high_throughput_no_acks_no_idemp_lz4(const char *brokers);
rd_kafka_t *create_producer_high_throughput_one_ack_no_idemp_snappy(const char *brokers);

// Latency
rd_kafka_t *create_producer_low_latency_acks_one_no_idemp(const char *brokers);
rd_kafka_t *create_producer_low_latency_no_acks_no_idemp(const char *brokers);


void with_stats_cb(rd_kafka_conf_t *config, int (*stats_cb)(rd_kafka_t *rk, char *json, size_t json_len, void *opaque));