#include <stdlib.h>
#include <stdio.h>

#include "producer_builder.h"

// TODO: Continue here, re-write the producer-building functions
// to be more builder-pattern ish
// Also, create functions for the different values and then
// for the different profiles specified in the report

rd_kafka_t *create_producer_basic(const char *brokers)
{
    char errstr[512];
    rd_kafka_conf_t *config = rd_kafka_conf_new();
    with_bootstrap_servers(config, brokers);
    rd_kafka_t *producer = create_producer_with_config(config);
    return producer;
}

rd_kafka_t *create_producer_ack_one(const char *brokers)
{
    char errstr[512];
    rd_kafka_conf_t *config = rd_kafka_conf_new();
    with_bootstrap_servers(config, brokers);
    with_acks_one(config);
    rd_kafka_t *producer = create_producer_with_config(config);
    return producer;
}

rd_kafka_t *create_producer_ack_all(const char *brokers)
{
    char errstr[512];
    rd_kafka_conf_t *config = rd_kafka_conf_new();
    with_bootstrap_servers(config, brokers);
    with_acks_all(config);
    rd_kafka_t *producer = create_producer_with_config(config);

    return producer;
}

rd_kafka_t *create_producer_high_throughput_all_acks_idemp_enabled_gzip(const char *brokers)
{
    char errstr[512];

    rd_kafka_conf_t *config = rd_kafka_conf_new();
    with_bootstrap_servers(config, brokers);
    with_queue_buffering_large(config);
    with_linger_time_long(config);
    with_batch_size_large(config);
    with_compression_gzip(config);
    with_acks_all(config);
    with_idempotence(config);

    rd_kafka_t *producer = create_producer_with_config(config);

    return producer;
}

rd_kafka_t *create_producer_high_throughput_all_acks_no_idemp_gzip(const char *brokers)
{
    char errstr[512];
    rd_kafka_conf_t *config = rd_kafka_conf_new();

    with_bootstrap_servers(config, brokers);
    with_queue_buffering_large(config);
    with_linger_time_long(config);
    with_batch_size_large(config);
    with_compression_gzip(config);
    with_acks_all(config);

    rd_kafka_t *producer = create_producer_with_config(config);
    return producer;
}

rd_kafka_t *create_producer_high_throughput_no_acks_no_idemp_gzip(const char *brokers)
{
    char errstr[512];
    rd_kafka_conf_t *config = rd_kafka_conf_new();

    with_bootstrap_servers(config, brokers);
    with_queue_buffering_large(config);
    with_linger_time_long(config);
    with_batch_size_large(config);
    with_compression_gzip(config);

    rd_kafka_t *producer = create_producer_with_config(config);
    return producer;
}

rd_kafka_t *create_producer_high_throughput_no_acks_no_idemp_lz4(const char *brokers)
{
    char errstr[512];
    rd_kafka_conf_t *config = rd_kafka_conf_new();

    with_bootstrap_servers(config, brokers);
    with_queue_buffering_large(config);
    with_linger_time_long(config);
    with_batch_size_large(config);
    with_compression_lz4(config);
    rd_kafka_t *producer = create_producer_with_config(config);
    return producer;
}

void with_idempotence(rd_kafka_conf_t *config)
{
    char errstr[512];

    if (rd_kafka_conf_set(config, "enable.idempotence", "true", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        exit(EXIT_FAILURE);
    }
}

void with_acks_all(rd_kafka_conf_t *config)
{
    char errstr[512];

    if (rd_kafka_conf_set(config, "acks", "all", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        exit(EXIT_FAILURE);
    }
}

void with_acks_one(rd_kafka_conf_t *config)
{
    char errstr[512];

    if (rd_kafka_conf_set(config, "acks", "1", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        exit(EXIT_FAILURE);
    }
}

void with_linger_time_long(rd_kafka_conf_t *config)
{
    char errstr[512];

    if (rd_kafka_conf_set(config, "linger.ms", "900000", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        exit(EXIT_FAILURE);
    }
}

void with_batch_size_large(rd_kafka_conf_t *config)
{
    char errstr[512];

    if (rd_kafka_conf_set(config, "batch.size", "2147483647", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        exit(EXIT_FAILURE);
    }
}

void with_compression_gzip(rd_kafka_conf_t *config)
{
    char errstr[512];

    if (rd_kafka_conf_set(config, "compression.type", "gzip", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        exit(EXIT_FAILURE);
    }
}

void with_compression_lz4(rd_kafka_conf_t *config)
{
    char errstr[512];

    if (rd_kafka_conf_set(config, "compression.type", "lz4", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        exit(EXIT_FAILURE);
    }
}

void with_queue_buffering_large(rd_kafka_conf_t *config)
{
    char errstr[512];

    if (rd_kafka_conf_set(config, "queue.buffering.max.kbytes", "2147483647", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        exit(EXIT_FAILURE);
    }

    if (rd_kafka_conf_set(config, "queue.buffering.max.messages", "10000000", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        exit(EXIT_FAILURE);
    }
}

void with_bootstrap_servers(rd_kafka_conf_t *config, char *brokers)
{
    char errstr[512];

    if (rd_kafka_conf_set(config, "bootstrap.servers", brokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        exit(EXIT_FAILURE);
    }
}

rd_kafka_t *create_producer_with_config(rd_kafka_conf_t *config)
{
    char errstr[512];
    rd_kafka_t *producer = rd_kafka_new(RD_KAFKA_PRODUCER, config, errstr, sizeof(errstr));
    if (!producer)
    {
        fprintf(stderr, "%% Failed to create new produer: %s\n", errstr);
        exit(EXIT_FAILURE);
    }

    return producer;
}