#include <librdkafka/rdkafka.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "kafka_utils.h"
#include "producer_builder.h"

#define VERY_LONG_TIME INT32_MAX
#define MAX_BROKERS 10

FILE *stats_fp = NULL;

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

void destroy_producer(rd_kafka_t *producer)
{
    rd_kafka_destroy(producer);
}

void flush_producer(rd_kafka_t *producer)
{

    fprintf(stderr, "%% Flushing final messages...\n");
    while (rd_kafka_outq_len(producer) > 0)
    {
        rd_kafka_flush(producer, VERY_LONG_TIME);
    }
    fprintf(stderr, "%% Flushed final messages.\n");

    // Don't destroy producers, adds time which would probably
    // not be needed otherwise, as destroying producers is rather
    // rare, not done often
    //rd_kafka_destroy(producer);
}

static void json_parse_stats(const char *json)
{
    if (stats_fp != NULL)
    {
        fprintf(stats_fp, "%s\n", json);
    }
}

int stats_cb(rd_kafka_t *rk, char *json, size_t json_len, void *opaque)
{
    json_parse_stats(json);
    return 0;
}

FILE *init_stats_fp(const char *filename)
{
    stats_fp = fopen(filename, "w");
    if (stats_fp == NULL)
    {
        fprintf(stderr, "Failed to init stats fp.\n");
        exit(EXIT_FAILURE);
    }
    return stats_fp;
}

void write_summary_stats(FILE *stats_fp, int cores, double elapsed_avg, size_t file_size)
{
    fprintf(stats_fp, "{ \"n_cores\": %d, \"elapsed_time_avg\": %f, \"bytes_sent\": %zu }\n", cores, elapsed_avg, file_size);
    fflush(stats_fp);
}


producer_info_t* init_new_producer_types(const char *brokers)
{
    producer_info_t *producer_infos = malloc(sizeof(producer_info_t) * NUM_NEW_PRODUCER_TYPES);
    producer_infos[0].producer = create_producer_medium_vals_no_acks_no_idemp_gzip(brokers);
    producer_infos[0].producer_name = "producer_medium_vals_no_acks_no_idemp_gzip";
    producer_infos[1].producer = create_producer_medium_vals_no_acks_no_idemp_lz4(brokers);
    producer_infos[1].producer_name = "producer_medium_vals_no_acks_no_idemp_lz4";
    producer_infos[2].producer = create_producer_medium_vals_no_acks_no_idemp_snappy(brokers);
    producer_infos[2].producer_name = "producer_medium_vals_no_acks_no_idemp_snappy";
    producer_infos[3].producer = create_producer_medium_vals_all_acks_idemp_enabled_gzip(brokers);
    producer_infos[3].producer_name = "producer_medium_vals_all_acks_idemp_enabled_gzip";
    producer_infos[4].producer = create_producer_medium_vals_all_acks_idemp_enabled_lz4(brokers);
    producer_infos[4].producer_name = "producer_medium_vals_all_acks_idemp_enabled_lz4";
    producer_infos[5].producer = create_producer_medium_vals_all_acks_idemp_enabled_snappy(brokers);
    producer_infos[5].producer_name = "producer_medium_vals_all_acks_idemp_enabled_snappy";
    producer_infos[6].producer = create_producer_ack_all_idemp_enabled(brokers);
    producer_infos[6].producer_name = "basic_producer_ack_all_idemp_enabled";

    return producer_infos;
}

// Should create one of each kind of producer
// Is kind of ass, but unlikely to change
producer_info_t *init_producers(const char *brokers)
{
    producer_info_t *producer_infos = malloc(sizeof(producer_info_t) * NUM_PRODUCER_TYPES);
    producer_infos[0].producer = create_producer_basic(brokers);
    producer_infos[0].producer_name = "basic_producer";
    producer_infos[1].producer = create_producer_ack_one(brokers);
    producer_infos[1].producer_name = "producer_ack_one";
    producer_infos[2].producer = create_producer_high_throughput_all_acks_idemp_enabled_gzip(brokers);
    producer_infos[2].producer_name = "producer_high_throughput_all_acks_idemp_enabled_gzip";
    producer_infos[3].producer = create_producer_high_throughput_all_acks_idemp_enabled_snappy(brokers);
    producer_infos[3].producer_name = "producer_high_throughput_all_acks_idemp_enabled_snappy";
    producer_infos[4].producer = create_producer_high_throughput_all_acks_idemp_enabled_lz4(brokers);
    producer_infos[4].producer_name = "producer_high_throughput_all_acks_idemp_enabled_lz4";
    producer_infos[5].producer = create_producer_high_throughput_all_acks_no_idemp_gzip(brokers);
    producer_infos[5].producer_name = "producer_high_throughput_all_acks_no_idemp_gzip";
    producer_infos[6].producer = create_producer_high_throughput_all_acks_no_idemp_snappy(brokers);
    producer_infos[6].producer_name = "producer_high_throughput_all_acks_no_idemp_snappy";
    producer_infos[7].producer = create_producer_high_throughput_all_acks_no_idemp_lz4(brokers);
    producer_infos[7].producer_name = "producer_high_throughput_all_acks_no_idemp_lz4";
    producer_infos[8].producer = create_producer_high_throughput_no_acks_no_idemp_gzip(brokers);
    producer_infos[8].producer_name = "producer_high_throughput_no_acks_no_idemp_gzip";
    producer_infos[9].producer = create_producer_high_throughput_no_acks_no_idemp_lz4(brokers);
    producer_infos[9].producer_name = "producer_high_throughput_no_acks_no_idemp_lz4";
    producer_infos[10].producer = create_producer_high_throughput_no_acks_no_idemp_snappy(brokers);
    producer_infos[10].producer_name = "producer_high_throughput_no_acks_no_idemp_snappy";
    producer_infos[11].producer = create_producer_medium_vals_no_acks_no_idemp_gzip(brokers);
    producer_infos[11].producer_name = "producer_medium_vals_no_acks_no_idemp_gzip";
    producer_infos[12].producer = create_producer_medium_vals_no_acks_no_idemp_lz4(brokers);
    producer_infos[12].producer_name = "producer_medium_vals_no_acks_no_idemp_lz4";
    producer_infos[13].producer = create_producer_medium_vals_no_acks_no_idemp_snappy(brokers);
    producer_infos[13].producer_name = "producer_medium_vals_no_acks_no_idemp_snappy";
    producer_infos[14].producer = create_producer_medium_vals_all_acks_idemp_enabled_gzip(brokers);
    producer_infos[14].producer_name = "producer_medium_vals_all_acks_idemp_enabled_gzip";
    producer_infos[15].producer = create_producer_medium_vals_all_acks_idemp_enabled_lz4(brokers);
    producer_infos[15].producer_name = "producer_medium_vals_all_acks_idemp_enabled_lz4";
    producer_infos[16].producer = create_producer_medium_vals_all_acks_idemp_enabled_snappy(brokers);
    producer_infos[16].producer_name = "producer_medium_vals_all_acks_idemp_enabled_snappy";
    producer_infos[17].producer = create_producer_ack_all_idemp_enabled(brokers);
    producer_infos[17].producer_name = "basic_producer_ack_all_idemp_enabled";
    // Should maybe try to do the same without compression?
    return producer_infos;
}

producer_info_t *init_producers_reverse_order(const char *brokers)
{
    producer_info_t *producer_infos = malloc(sizeof(producer_info_t) * NUM_PRODUCER_TYPES);
    producer_infos[17].producer = create_producer_basic(brokers);
    producer_infos[17].producer_name = "basic_producer";
    producer_infos[16].producer = create_producer_ack_one(brokers);
    producer_infos[16].producer_name = "producer_ack_one";
    producer_infos[15].producer = create_producer_high_throughput_all_acks_idemp_enabled_gzip(brokers);
    producer_infos[15].producer_name = "producer_high_throughput_all_acks_idemp_enabled_gzip";
    producer_infos[14].producer = create_producer_high_throughput_all_acks_idemp_enabled_snappy(brokers);
    producer_infos[14].producer_name = "producer_high_throughput_all_acks_idemp_enabled_snappy";
    producer_infos[13].producer = create_producer_high_throughput_all_acks_idemp_enabled_lz4(brokers);
    producer_infos[13].producer_name = "producer_high_throughput_all_acks_idemp_enabled_lz4";
    producer_infos[12].producer = create_producer_high_throughput_all_acks_no_idemp_gzip(brokers);
    producer_infos[12].producer_name = "producer_high_throughput_all_acks_no_idemp_gzip";
    producer_infos[11].producer = create_producer_high_throughput_all_acks_no_idemp_snappy(brokers);
    producer_infos[11].producer_name = "producer_high_throughput_all_acks_no_idemp_snappy";
    producer_infos[10].producer = create_producer_high_throughput_all_acks_no_idemp_lz4(brokers);
    producer_infos[10].producer_name = "producer_high_throughput_all_acks_no_idemp_lz4";
    producer_infos[9].producer = create_producer_high_throughput_no_acks_no_idemp_gzip(brokers);
    producer_infos[9].producer_name = "producer_high_throughput_no_acks_no_idemp_gzip";
    producer_infos[8].producer = create_producer_high_throughput_no_acks_no_idemp_lz4(brokers);
    producer_infos[8].producer_name = "producer_high_throughput_no_acks_no_idemp_lz4";
    producer_infos[7].producer = create_producer_high_throughput_no_acks_no_idemp_snappy(brokers);
    producer_infos[7].producer_name = "producer_high_throughput_no_acks_no_idemp_snappy";
    producer_infos[6].producer = create_producer_medium_vals_no_acks_no_idemp_gzip(brokers);
    producer_infos[6].producer_name = "producer_medium_vals_no_acks_no_idemp_gzip";
    producer_infos[5].producer = create_producer_medium_vals_no_acks_no_idemp_lz4(brokers);
    producer_infos[5].producer_name = "producer_medium_vals_no_acks_no_idemp_lz4";
    producer_infos[4].producer = create_producer_medium_vals_no_acks_no_idemp_snappy(brokers);
    producer_infos[4].producer_name = "producer_medium_vals_no_acks_no_idemp_snappy";
    producer_infos[3].producer = create_producer_medium_vals_all_acks_idemp_enabled_gzip(brokers);
    producer_infos[3].producer_name = "producer_medium_vals_all_acks_idemp_enabled_gzip";
    producer_infos[2].producer = create_producer_medium_vals_all_acks_idemp_enabled_lz4(brokers);
    producer_infos[2].producer_name = "producer_medium_vals_all_acks_idemp_enabled_lz4";
    producer_infos[1].producer = create_producer_medium_vals_all_acks_idemp_enabled_snappy(brokers);
    producer_infos[1].producer_name = "producer_medium_vals_all_acks_idemp_enabled_snappy";
    producer_infos[0].producer = create_producer_ack_all_idemp_enabled(brokers);
    producer_infos[0].producer_name = "basic_producer_ack_all_idemp_enabled";
    // Should maybe try to do the same without compression?
    return producer_infos;

}