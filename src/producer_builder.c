#include <stdlib.h>
#include <stdio.h>

#include "producer_builder.h"
#include "kafka_utils.h"

#define LOG_MS_INTERVAL "2000" // milliseconds

rd_kafka_t *create_producer_basic(const char *brokers)
{
    rd_kafka_conf_t *config = rd_kafka_conf_new();
    with_bootstrap_servers(config, brokers);
    with_stats_cb(config, stats_cb);

    rd_kafka_t *producer = create_producer_with_config(config);
    return producer;
}

rd_kafka_t *create_producer_ack_one(const char *brokers)
{
    rd_kafka_conf_t *config = rd_kafka_conf_new();
    with_bootstrap_servers(config, brokers);
    with_acks_one(config);
    with_stats_cb(config, stats_cb);

    rd_kafka_t *producer = create_producer_with_config(config);
    return producer;
}

rd_kafka_t *create_producer_high_throughput_all_acks_idemp_enabled_gzip(const char *brokers)
{
    rd_kafka_conf_t *config = rd_kafka_conf_new();
    with_bootstrap_servers(config, brokers);
    with_queue_buffering_large(config);
    with_linger_time_long(config);
    with_batch_size_large(config);
    with_compression_gzip(config);
    with_acks_all(config);
    with_idempotence(config);
    with_stats_cb(config, stats_cb);

    rd_kafka_t *producer = create_producer_with_config(config);

    return producer;
}

rd_kafka_t *create_producer_high_throughput_one_ack_no_idemp_snappy(const char *brokers)
{
    rd_kafka_conf_t *config = rd_kafka_conf_new();
    with_bootstrap_servers(config, brokers);
    with_queue_buffering_large(config);
    with_linger_time_long(config);
    with_batch_size_large(config);
    with_compression_snappy(config);
    with_acks_one(config);
    with_stats_cb(config, stats_cb);

    rd_kafka_t *producer = create_producer_with_config(config);

    return producer;
}

rd_kafka_t *create_producer_high_throughput_all_acks_idemp_enabled_snappy(const char *brokers)
{
    rd_kafka_conf_t *config = rd_kafka_conf_new();
    with_bootstrap_servers(config, brokers);
    with_queue_buffering_large(config);
    with_linger_time_long(config);
    with_batch_size_large(config);
    with_compression_snappy(config);
    with_acks_all(config);
    with_idempotence(config);
    with_stats_cb(config, stats_cb);

    rd_kafka_t *producer = create_producer_with_config(config);

    return producer;
}

rd_kafka_t *create_producer_high_throughput_all_acks_idemp_enabled_lz4(const char *brokers)
{
    rd_kafka_conf_t *config = rd_kafka_conf_new();
    with_bootstrap_servers(config, brokers);
    with_queue_buffering_large(config);
    with_linger_time_long(config);
    with_batch_size_large(config);
    with_compression_lz4(config);
    with_acks_all(config);
    with_idempotence(config);
    with_stats_cb(config, stats_cb);

    rd_kafka_t *producer = create_producer_with_config(config);

    return producer;
}

rd_kafka_t *create_producer_high_throughput_all_acks_no_idemp_gzip(const char *brokers)
{
    rd_kafka_conf_t *config = rd_kafka_conf_new();

    with_bootstrap_servers(config, brokers);
    with_queue_buffering_large(config);
    with_linger_time_long(config);
    with_batch_size_large(config);
    with_compression_gzip(config);
    with_acks_all(config);
    with_stats_cb(config, stats_cb);

    rd_kafka_t *producer = create_producer_with_config(config);
    return producer;
}

rd_kafka_t *create_producer_high_throughput_all_acks_no_idemp_snappy(const char *brokers)
{
    rd_kafka_conf_t *config = rd_kafka_conf_new();

    with_bootstrap_servers(config, brokers);
    with_queue_buffering_large(config);
    with_linger_time_long(config);
    with_batch_size_large(config);
    with_compression_snappy(config);
    with_acks_all(config);
    with_stats_cb(config, stats_cb);

    rd_kafka_t *producer = create_producer_with_config(config);
    return producer;
}

rd_kafka_t *create_producer_high_throughput_all_acks_no_idemp_lz4(const char *brokers)
{
    rd_kafka_conf_t *config = rd_kafka_conf_new();

    with_bootstrap_servers(config, brokers);
    with_queue_buffering_large(config);
    with_linger_time_long(config);
    with_batch_size_large(config);
    with_compression_lz4(config);
    with_acks_all(config);
    with_stats_cb(config, stats_cb);

    rd_kafka_t *producer = create_producer_with_config(config);
    return producer;
}


rd_kafka_t *create_producer_high_throughput_no_acks_no_idemp_gzip(const char *brokers)
{
    rd_kafka_conf_t *config = rd_kafka_conf_new();

    with_bootstrap_servers(config, brokers);
    with_queue_buffering_large(config);
    with_linger_time_long(config);
    with_batch_size_large(config);
    with_compression_gzip(config);
    with_stats_cb(config, stats_cb);

    rd_kafka_t *producer = create_producer_with_config(config);
    return producer;
}

rd_kafka_t *create_producer_high_throughput_no_acks_no_idemp_lz4(const char *brokers)
{
    rd_kafka_conf_t *config = rd_kafka_conf_new();

    with_bootstrap_servers(config, brokers);
    with_queue_buffering_large(config);
    with_linger_time_long(config);
    with_batch_size_large(config);
    with_compression_lz4(config);
    with_stats_cb(config, stats_cb);

    rd_kafka_t *producer = create_producer_with_config(config);
    return producer;
}

rd_kafka_t *create_producer_high_throughput_no_acks_no_idemp_snappy(const char *brokers)
{
    rd_kafka_conf_t *config = rd_kafka_conf_new();

    with_bootstrap_servers(config, brokers);
    with_queue_buffering_large(config);
    with_linger_time_long(config);
    with_batch_size_large(config);
    with_compression_snappy(config);
    with_stats_cb(config, stats_cb);

    rd_kafka_t *producer = create_producer_with_config(config);
    return producer;
}

rd_kafka_t *create_producer_low_latency_acks_one_no_idemp(const char *brokers)
{
    rd_kafka_conf_t *config = rd_kafka_conf_new();

    with_bootstrap_servers(config, brokers);
    with_linger_time_zero(config);
    with_batch_size_small(config);
    with_compression_none(config);
    with_acks_one(config);
    with_stats_cb(config, stats_cb);

    rd_kafka_t *producer = create_producer_with_config(config);
    return producer;
}

rd_kafka_t *create_producer_low_latency_no_acks_no_idemp(const char *brokers)
{
    rd_kafka_conf_t *config = rd_kafka_conf_new();

    with_bootstrap_servers(config, brokers);
    with_linger_time_zero(config);
    with_batch_size_small(config);
    with_compression_none(config);
    without_acks(config);
    with_stats_cb(config, stats_cb);

    rd_kafka_t *producer = create_producer_with_config(config);
    return producer;
}


rd_kafka_t *create_producer_low_latency_acks_all_idemp_enabled(const char *brokers)
{
    rd_kafka_conf_t *config = rd_kafka_conf_new();

    with_bootstrap_servers(config, brokers);
    with_linger_time_zero(config);
    with_batch_size_small(config);
    with_compression_none(config);
    with_acks_all(config);
    with_idempotence(config);
    with_stats_cb(config, stats_cb);

    rd_kafka_t *producer = create_producer_with_config(config);
    return producer;
}

rd_kafka_t *create_producer_low_latency_no_acks_no_idemp_snappy(const char *brokers)
{
    rd_kafka_conf_t *config = rd_kafka_conf_new();

    with_bootstrap_servers(config, brokers);
    with_linger_time_zero(config);
    with_batch_size_small(config);
    with_compression_snappy(config);
    without_acks(config);
    with_stats_cb(config, stats_cb);
    rd_kafka_t *producer = create_producer_with_config(config);
    return producer;
}

rd_kafka_t *create_producer_low_latency_no_acks_no_idemp_gzip(const char *brokers)
{
    rd_kafka_conf_t *config = rd_kafka_conf_new();

    with_bootstrap_servers(config, brokers);
    with_linger_time_zero(config);
    with_batch_size_small(config);
    with_compression_gzip(config);
    without_acks(config);
    with_stats_cb(config, stats_cb);
    rd_kafka_t *producer = create_producer_with_config(config);
    return producer;
}

rd_kafka_t *create_producer_low_latency_no_acks_no_idemp_lz4(const char *brokers)
{
    rd_kafka_conf_t *config = rd_kafka_conf_new();

    with_bootstrap_servers(config, brokers);
    with_linger_time_zero(config);
    with_batch_size_small(config);
    with_compression_lz4(config);
    without_acks(config);
    with_stats_cb(config, stats_cb);
    rd_kafka_t *producer = create_producer_with_config(config);
    return producer;
}

void with_stats_cb(rd_kafka_conf_t *config, int (*stats_cb)(rd_kafka_t *rk, char *json, size_t json_len, void *opaque))
{
    rd_kafka_conf_set_stats_cb(config, stats_cb);
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

void without_acks(rd_kafka_conf_t *config)
{
    char errstr[512];

    if (rd_kafka_conf_set(config, "acks", "0", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
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

void with_linger_time_100(rd_kafka_conf_t *config)
{
    char errstr[512];

    if (rd_kafka_conf_set(config, "linger.ms", "100", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        exit(EXIT_FAILURE);
    }
}

void with_linger_time_10(rd_kafka_conf_t *config)
{
    char errstr[512];

    if (rd_kafka_conf_set(config, "linger.ms", "10", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        exit(EXIT_FAILURE);
    }
}

void with_linger_time_zero(rd_kafka_conf_t *config)
{
    char errstr[512];

    if (rd_kafka_conf_set(config, "linger.ms", "0", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
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

void with_batch_size_medium(rd_kafka_conf_t *config)
{
    char errstr[512];

    if (rd_kafka_conf_set(config, "batch.size", "1048576", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        exit(EXIT_FAILURE);
    }
}

void with_batch_size_small(rd_kafka_conf_t *config)
{
    char errstr[512];

    if (rd_kafka_conf_set(config, "batch.size", "16384", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
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

void with_compression_snappy(rd_kafka_conf_t *config)
{
    char errstr[512];

    if (rd_kafka_conf_set(config, "compression.type", "snappy", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        exit(EXIT_FAILURE);
    }
}

void with_compression_none(rd_kafka_conf_t *config)
{
    char errstr[512];

    if (rd_kafka_conf_set(config, "compression.type", "none", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
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

    // This can always be set to the maximum value, as max.kbytes has a higher priority
    if (rd_kafka_conf_set(config, "queue.buffering.max.messages", "10000000", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        exit(EXIT_FAILURE);
    }
}

void with_queue_buffering_medium(rd_kafka_conf_t *config)
{
    char errstr[512];

    if (rd_kafka_conf_set(config, "queue.buffering.max.kbytes", "1048576", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        exit(EXIT_FAILURE);
    }

    // This can always be set to the maximum value, as max.kbytes has a higher priority
    if (rd_kafka_conf_set(config, "queue.buffering.max.messages", "10000000", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        exit(EXIT_FAILURE);
    }
}

void with_queue_buffering_small(rd_kafka_conf_t *config)
{
    char errstr[512];

    if (rd_kafka_conf_set(config, "queue.buffering.max.kbytes", "16384", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        exit(EXIT_FAILURE);
    }

    // This can always be set to the maximum value, as max.kbytes has a higher priority
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

void with_logging_interval_ms(rd_kafka_conf_t *config)
{
    char errstr[512];

    if (rd_kafka_conf_set(config, "statistics.interval.ms", LOG_MS_INTERVAL, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        exit(EXIT_FAILURE);
    }
}

rd_kafka_t *create_producer_with_config(rd_kafka_conf_t *config)
{

    with_logging_interval_ms(config);

    char errstr[512];
    rd_kafka_t *producer = rd_kafka_new(RD_KAFKA_PRODUCER, config, errstr, sizeof(errstr));
    if (!producer)
    {
        fprintf(stderr, "%% Failed to create new produer: %s\n", errstr);
        exit(EXIT_FAILURE);
    }

    return producer;
}