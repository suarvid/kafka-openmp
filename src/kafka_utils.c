#include <librdkafka/rdkafka.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#define VERY_LONG_TIME INT32_MAX

static FILE *stats_fp;

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

void flush_destroy_producer(rd_kafka_t *producer)
{

    fprintf(stderr, "%% Flushing final messages...\n");
    while (rd_kafka_outq_len(producer) > 0)
    {
        rd_kafka_flush(producer, VERY_LONG_TIME);
    }
    fprintf(stderr, "%% Flushed final messages.\n");

    rd_kafka_destroy(producer);
}

// Should contain all the metrics we care about
static struct
{
    uint64_t avg_rtt;
} stats;

// Taken from librdkafka example
static uint64_t json_parse_fields(
    const char *json,
    const char **end,
    const char *field1,
    const char *field2)
{
    const char *t = json;
    const char *t2;
    int len1 = (int)strlen(field1);
    int len2 = (int)strlen(field2);

    while ((t2 = strstr(t, field1)))
    {
        uint64_t v;

        t = t2;
        t += len1;

        // Find field
        if (!(t2 = strstr(t, field2)))
        {
            continue;
        }
        t2 += len2;

        while (isspace((int)*t2))
        {
            t2++;
        }

        v = strtoull(t2, (char **)&t, 10);
        if (t2 == t)
        {
            continue;
        }
        *end = t;
        return v;
    }

    *end = t + strlen(t);
    return 0;
}

// Based on librdkafka example
static void json_parse_stats(const char *json)
{
    const char *t;
#define MAX_BROKERS 10
    uint64_t avg_rtt[MAX_BROKERS + 1];
    int avg_rtt_i = 0;

    // Keep the total in the last element
    avg_rtt[MAX_BROKERS] = 0;

    t = json;
    while (avg_rtt_i < MAX_BROKERS && *t)
    {
        avg_rtt[avg_rtt_i] = json_parse_fields(t, &t, "\"rtt\":", "\"avg\":");

        if (avg_rtt[avg_rtt_i] < 100)
        {
            continue;
        }

        avg_rtt[MAX_BROKERS] += avg_rtt[avg_rtt_i];
        avg_rtt_i++;
    }

    if (avg_rtt_i > 0)
    {
        avg_rtt[MAX_BROKERS] /= avg_rtt_i;
    }

    stats.avg_rtt = avg_rtt[MAX_BROKERS];
}

static int stats_cb(rd_kafka_t *rk, char *json, size_t json_len, void *opaque)
{
    json_parse_stats(json);

    if (stats_fp)
    {
        fprintf(stats_fp, "%s\n", json);
    }

    return 0;
}