#pragma once

#include <librdkafka/rdkafka.h>


rd_kafka_t *create_with_config(rd_kafka_conf_t *config);