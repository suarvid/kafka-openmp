#pragma once

#include <stdio.h>
#include <librdkafka/rdkafka.h>

struct thread_args
{
    char *buffer;
    int start_point;
    int messages_to_read;
    char *topic;
    char *broker;
};

struct omp_thread_args_private_producer
{
    char *buffer;
    int messages_per_thread;
    char *topic;
    char *broker;
};

struct omp_thread_args_shared_producer
{
    char *buffer;
    int messages_per_thread;
    char *topic;
    char *broker;
    rd_kafka_t *producer;
};

void publish_with_n_cores(FILE *fp, char *broker, char *topic, int n_cores);
size_t publish_with_omp_private_producer(const FILE *fp, const char *brokers, const char *topic, int n_threads);
size_t publish_with_omp_shared_producer(const FILE *fp, const char *brokers, const char *topic, int n_threads);