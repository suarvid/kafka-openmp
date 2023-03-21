#pragma once
#include <librdkafka/rdkafka.h>

#define MESSAGE_SIZE 32

rd_kafka_t *create_producer_basic(const char *brokers);
rd_kafka_resp_err_t send_message(rd_kafka_t *producer, const char *topic, char *buf, size_t len);
void flush_destroy_producer(rd_kafka_t *producer);