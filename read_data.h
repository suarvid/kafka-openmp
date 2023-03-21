#pragma once
#include <librdkafka/rdkafka.h>
#include <stdio.h>
#include <stdlib.h>

void publish_line_by_line(rd_kafka_t *producer,
                          FILE *fp, char *topic);