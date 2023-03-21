#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <librdkafka/rdkafka.h>

ssize_t read_line(FILE *fp, char *linebuf)
{
    size_t len = 0;
    ssize_t read;

    if (fp == NULL)
    {
        fprintf(stderr, "Got null file pointer in read_line!");
        exit(EXIT_FAILURE);
    }

    read = getline(&linebuf, &len, fp);
    return read;
}

void publish_line_by_line(rd_kafka_t *producer,
                          FILE *fp, char *topic)
{
    char buf[512];
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