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

void publish_with_n_cores(FILE *fp, char *broker, char *topic, int n_cores);