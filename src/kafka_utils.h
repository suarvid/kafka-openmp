#pragma once
#include <librdkafka/rdkafka.h>

#define MESSAGE_SIZE 1024 // Size of each message, in bytes
#define MEASUREMENTS_PER_RUN 5
#define SLEEP_BETWEEN_MEASUREMENTS 20 // seconds

typedef struct producer_info
{
    rd_kafka_t *producer;
    char *producer_name;
} producer_info_t;

rd_kafka_t *create_producer_basic(const char *brokers);
rd_kafka_t *create_producer_ack_one(const char *brokers);
rd_kafka_resp_err_t send_message(rd_kafka_t *producer, const char *topic, char *buf, size_t len);
void flush_producer(rd_kafka_t *producer);
rd_kafka_t *create_producer_high_throughput_no_acks_no_idemp_gzip(const char *brokers);
rd_kafka_t *create_producer_high_throughput_all_acks_no_idemp_gzip(const char *brokers);
rd_kafka_t *create_producer_high_throughput_all_acks_idemp_enabled_gzip(const char *brokers);
rd_kafka_t *create_producer_high_throughput_no_acks_no_idemp_lz4(const char *brokers);

// TODO: Maybe add functions for creating a producer for each profile to be tested
int stats_cb(rd_kafka_t *rk, char *json, size_t json_len, void *opaque);
FILE *init_stats_fp(const char *filename);
void write_summary_stats(FILE *stats_fp, int cores, double elapsed_avg, size_t file_size);