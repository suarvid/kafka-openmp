#pragma once
#include <librdkafka/rdkafka.h>

#define MESSAGE_SIZE 32

rd_kafka_t *create_producer_basic(const char *brokers);
rd_kafka_t *create_producer_ack_one(const char *brokers);
rd_kafka_resp_err_t send_message(rd_kafka_t *producer, const char *topic, char *buf, size_t len);
void flush_destroy_producer(rd_kafka_t *producer);
rd_kafka_t *create_producer_high_throughput_no_acks_no_idemp_gzip(const char *brokers);
rd_kafka_t *create_producer_high_throughput_all_acks_no_idemp_gzip(const char *brokers);
rd_kafka_t *create_producer_high_throughput_all_acks_idemp_enabled_gzip(const char *brokers);
rd_kafka_t *create_producer_high_throughput_no_acks_no_idemp_lz4(const char *brokers);

// TODO: Maybe add functions for creating a producer for each profile to be tested
int stats_cb(rd_kafka_t *rk, char *json, size_t json_len, void *opaque);
FILE *init_stats_fp(const char *filename);