#include <librdkafka/rdkafka.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#define VERY_LONG_TIME INT32_MAX
#define MAX_BROKERS 10

FILE *stats_fp = NULL;

rd_kafka_resp_err_t send_message(rd_kafka_t *producer, const char *topic, char *buf, size_t len)
{
    rd_kafka_resp_err_t err;
    err = rd_kafka_producev(
        producer,
        RD_KAFKA_V_TOPIC(topic),
        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
        RD_KAFKA_V_VALUE(buf, len),
        RD_KAFKA_V_OPAQUE(NULL),
        RD_KAFKA_V_END);

    rd_kafka_poll(producer, 0);

    return err;
}

void destroy_producer(rd_kafka_t *producer)
{
    rd_kafka_destroy(producer);
}

void flush_producer(rd_kafka_t *producer)
{

    fprintf(stderr, "%% Flushing final messages...\n");
    while (rd_kafka_outq_len(producer) > 0)
    {
        rd_kafka_flush(producer, VERY_LONG_TIME);
    }
    fprintf(stderr, "%% Flushed final messages.\n");

    // Don't destroy producers, adds time which would probably
    // not be needed otherwise, as destroying producers is rather
    // rare, not done often
    //rd_kafka_destroy(producer);
}

static void json_parse_stats(const char *json)
{
    if (stats_fp != NULL)
    {
        fprintf(stats_fp, "%s\n", json);
    }
}

int stats_cb(rd_kafka_t *rk, char *json, size_t json_len, void *opaque)
{
    json_parse_stats(json);
    return 0;
}

FILE *init_stats_fp(const char *filename)
{
    stats_fp = fopen(filename, "w");
    if (stats_fp == NULL)
    {
        fprintf(stderr, "Failed to init stats fp.\n");
        exit(EXIT_FAILURE);
    }
    return stats_fp;
}