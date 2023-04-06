#pragma once

#include <stdio.h>
#include <librdkafka/rdkafka.h>

struct thread_args
{
    char *buffer;
    int start_point;
    int messages_to_read;
    char *topic;
    char *brokers;
};

struct omp_thread_args_private_producer
{
    char *buffer;
    int messages_per_thread;
    char *topic;
    char *brokers;
};

struct omp_thread_args_shared_producer
{
    char *buffer;
    int messages_per_thread;
    char *topic;
    char *brokers;
    rd_kafka_t *producer;
    char *producer_name;
    int producer_name_len;
};

typedef struct producer_info
{
    rd_kafka_t *producer;
    char *producer_name;
} producer_info_t;

void publish_parallel_for_shared(size_t file_size, char* buffer, rd_kafka_t *producer, char* topic, int n_cores);
void publish_parallel_for_private(size_t file_size, char* buffer, producer_info_t **producer_infos, int producer_idx, char* topic, int n_cores);