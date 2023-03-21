#include <librdkafka/rdkafka.h>
#include <stdlib.h>

rd_kafka_t *create_producer_basic(const char *brokers)
{
    char errstr[512];
    rd_kafka_conf_t *config = rd_kafka_conf_new();

    if (rd_kafka_conf_set(config, "bootstrap.servers", brokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        exit(EXIT_FAILURE);
    }

    //rd_kafka_conf_set_dr_msg_cb(config, NULL);

    rd_kafka_t *producer = rd_kafka_new(RD_KAFKA_PRODUCER, config, errstr, sizeof(errstr));
    if (!producer)
    {
        fprintf(stderr, "%% Failed to create new produer: %s\n", errstr);
        exit(EXIT_FAILURE);
    }

    return producer;
}

rd_kafka_t *create_producer_with_callback(const char *brokers, void *callback_fn)
{

}

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
    rd_kafka_flush(producer, 10 * 1000);

    if (rd_kafka_outq_len(producer) > 0)
    {
        fprintf(stderr, "%% %d messages(s) were not delivered\n",
                rd_kafka_outq_len(producer));
    }

    rd_kafka_destroy(producer);
}