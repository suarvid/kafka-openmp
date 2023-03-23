#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

#include <librdkafka/rdkafka.h>
#include "read_data.h"
#include "parallel_producer.h"
#include "kafka_utils.h"
#include <omp.h>

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
    int n_requested_cores;

    if (argc != 5)
    {
        fprintf(stderr, "%% Usage: %s <broker> <topic> <inputfile> <n_cores> \n", argv[0]);
        return EXIT_FAILURE;
    }

    brokers = argv[1];
    topic = argv[2];
    input_file = argv[3];
    n_requested_cores = atoi(argv[4]);


    FILE *fp = fopen(input_file, "rb");

    if (fp == NULL)
    {
        fprintf(stderr, "Failed to open file!\n");
        exit(EXIT_FAILURE);
    }

    double start;
    double end;
    double elapsed;

    size_t bytes_sent;

    start = omp_get_wtime();
    bytes_sent = publish_with_omp_shared_producer(fp, brokers, topic, n_requested_cores);
    end = omp_get_wtime();
    elapsed = end - start;
    fprintf(stderr, "Sent %zu bytes in %f seconds with shared producer.\n", bytes_sent, elapsed);

    start = omp_get_wtime();
    bytes_sent = publish_with_omp_private_producer(fp, brokers, topic, n_requested_cores);
    end = omp_get_wtime();
    elapsed = end - start;
    fprintf(stderr, "Sent %zu bytes in %f seconds with private producers.\n", bytes_sent, elapsed);

    fclose(fp);

    return 0;
}