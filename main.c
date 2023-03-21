#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

#include <librdkafka/rdkafka.h>
#include "read_data.h"
#include "parallel_producer.h"
#include "kafka_utils.h"

static volatile sig_atomic_t run = 1;

static void stop(int sig)
{
    run = 0;
    fclose(stdin);
}

static void callback(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque)
{
    if (rkmessage->err)
    {
        fprintf(stderr, "%% Message delivery failed: %s\n", rd_kafka_err2str(rkmessage->err));
    }
    // rkmessage destroyed automagically by kafka
}

int main(int argc, char **argv)
{
    const char *brokers;
    const char *topic;
    const char *input_file;

    if (argc != 4)
    {
        fprintf(stderr, "%% Usage: %s <broker> <topic> <inputfile> \n", argv[0]);
        return EXIT_FAILURE;
    }

    brokers = argv[1];
    topic = argv[2];
    input_file = argv[3];

    FILE *fp = fopen(input_file, "r");

    if (fp == NULL)
    {
        fprintf(stderr, "Failed to open file!\n");
        exit(EXIT_FAILURE);
    }

    publish_in_parallel(fp, brokers, topic);
    //publish_sequential(brokers, topic, input_file);

    return 0;
}

void publish_sequential(char *brokers, char* topic, char* input_file) 
{
    rd_kafka_t *producer;
    rd_kafka_conf_t *config;
    char errstr[512];
    char buf[512];

    config = rd_kafka_conf_new();

    if (rd_kafka_conf_set(config, "bootstrap.servers", brokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        exit(EXIT_FAILURE);
    }

    rd_kafka_conf_set_dr_msg_cb(config, callback);

    producer = rd_kafka_new(RD_KAFKA_PRODUCER, config, errstr, sizeof(errstr));
    if (!producer)
    {
        fprintf(stderr, "%% Failed to create new produer: %s\n", errstr);
        return EXIT_FAILURE;
    }

    signal(SIGINT, stop);

    FILE *fp = fopen(input_file, "r");
    if (fp == NULL)
    {
        fprintf(stderr, "Failed to open file!\n");
        exit(EXIT_FAILURE);
    }

    clock_t start;
    clock_t end;
    start = clock();
    publish_line_by_line(producer, fp, topic);
    end = clock();

    double elapsed = (double)(end - start) / CLOCKS_PER_SEC;

    printf("Exeuction time: %f seconds.\n", elapsed);

    fprintf(stderr, "%% Flushing final messages...\n");
    rd_kafka_flush(producer, 10 * 1000);

    if (rd_kafka_outq_len(producer) > 0)
    {
        fprintf(stderr, "%% %d messages(s) were not delivered\n",
                rd_kafka_outq_len(producer));
    }

    rd_kafka_destroy(producer);

}

void publish_line_by_line(rd_kafka_t *producer,
                          FILE *fp, char *topic)
{
    char buf[MESSAGE_SIZE];
    ssize_t len;
    rd_kafka_resp_err_t err;

    do
    {
        len = read_line(fp, buf);
        if (buf[len - 1] == '\n')
        {
            buf[len - 1] = '\0';
            len -= 1;
        }
        err = rd_kafka_producev(
            producer,
            RD_KAFKA_V_TOPIC(topic),
            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
            RD_KAFKA_V_VALUE(buf, len),
            RD_KAFKA_V_OPAQUE(NULL),
            RD_KAFKA_V_END);

        rd_kafka_poll(producer, 0);

    } while (len != -1);
}
