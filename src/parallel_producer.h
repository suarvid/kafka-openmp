#pragma once

#include <stdio.h>

struct thread_args
{
    char *buffer;
    int start_point;
    int messages_to_read;
    char *topic;
    char *broker;
};

struct omp_thread_args
{
    char *buffer;
    int messages_per_thread;
    char *topic;
    char *broker;
};

void publish_with_n_cores(FILE *fp, char *broker, char *topic, int n_cores);
size_t publish_with_omp(const FILE *fp, const char *brokers, const char *topic, int n_threads);
void omp_thread_process_data(struct omp_thread_args args);