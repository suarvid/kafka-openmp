#include <librdkafka/rdkafka.h>
#include <stdlib.h>

#define VERY_LONG_TIME INT32_MAX

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