#pragma once

#include <librdkafka/rdkafka.h>

#define NUM_PRODUCER_TYPES 18 // kinda sketch but
#define NUM_NEW_PRODUCER_TYPES 7
#define FINAL_NUM_PRODUCER_TYPES 6

// Okay, so the only types we should use are
// 1. Basic, no acks, no idemp
// 2. Basic, acks, idemp
// 3. Medium settings, no acks, no idemp
// 4. Medium settings, acks, idemp
// 5. Maximized settings, no acks, no idemp
// 6. Maximized settings, acks, idemp
// Maybe just use gzip for each kind? We have some data
// comparing gzip, snappy and lz4 already, so we can use that
// once we've shown that outbuf latency is the limiting factor
// in most cases
rd_kafka_t *create_producer_basic_gzip(const char *brokers);
rd_kafka_t *create_producer_ack_all_idemp_enabled_gzip(const char *brokers);



// Throughput
rd_kafka_t *create_producer_with_config(rd_kafka_conf_t *config);
rd_kafka_t *create_producer_basic(const char *brokers);
rd_kafka_t *create_producer_ack_one(const char *brokers);
rd_kafka_t *create_producer_high_throughput_no_acks_no_idemp_snappy(const char *brokers);
rd_kafka_t *create_producer_high_throughput_no_acks_no_idemp_gzip(const char *brokers);
rd_kafka_t *create_producer_high_throughput_no_acks_no_idemp_lz4(const char *brokers);
rd_kafka_t *create_producer_high_throughput_all_acks_idemp_enabled_snappy(const char *brokers);
rd_kafka_t *create_producer_high_throughput_all_acks_idemp_enabled_lz4(const char *brokers);
rd_kafka_t *create_producer_high_throughput_all_acks_idemp_enabled_gzip(const char *brokers);
rd_kafka_t *create_producer_high_throughput_all_acks_no_idemp_gzip(const char *brokers);
rd_kafka_t *create_producer_high_throughput_all_acks_no_idemp_snappy(const char *brokers);
rd_kafka_t *create_producer_high_throughput_all_acks_no_idemp_lz4(const char *brokers);

// New profiles, added as an afterthought sort of
rd_kafka_t *create_producer_medium_vals_no_acks_no_idemp_gzip(const char *brokers);
rd_kafka_t *create_producer_medium_vals_no_acks_no_idemp_lz4(const char *brokers);
rd_kafka_t *create_producer_medium_vals_no_acks_no_idemp_snappy(const char *brokers);
rd_kafka_t *create_producer_medium_vals_all_acks_idemp_enabled_gzip(const char *brokers);
rd_kafka_t *create_producer_medium_vals_all_acks_idemp_enabled_lz4(const char *brokers);
rd_kafka_t *create_producer_medium_vals_all_acks_idemp_enabled_snappy(const char *brokers);
rd_kafka_t *create_producer_ack_all_idemp_enabled(const char *brokers);
// Latency, not really used anymore
rd_kafka_t *create_producer_low_latency_acks_one_no_idemp(const char *brokers);
rd_kafka_t *create_producer_low_latency_no_acks_no_idemp(const char *brokers);
rd_kafka_t *create_producer_low_latency_acks_all_idemp_enabled(const char *brokers);
rd_kafka_t *create_producer_low_latency_no_acks_no_idemp_snappy(const char *brokers);
rd_kafka_t *create_producer_low_latency_no_acks_no_idemp_gzip(const char *brokers);
rd_kafka_t *create_producer_low_latency_no_acks_no_idemp_lz4(const char *brokers);

void with_stats_cb(rd_kafka_conf_t *config, int (*stats_cb)(rd_kafka_t *rk, char *json, size_t json_len, void *opaque));