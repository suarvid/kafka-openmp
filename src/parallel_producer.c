#include <stdio.h>
#include <librdkafka/rdkafka.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <omp.h>

#include "read_data.h"
#include "kafka_utils.h"
#include "parallel_producer.h"
#include "producer_builder.h"

#define SHORT_SLEEP 100000

int get_num_cores();

void publish_parallel_for_shared(size_t file_size, char* buffer, rd_kafka_t *producer, char* topic, int n_cores)
{
    rd_kafka_resp_err_t err;
#pragma omp parallel for num_threads(n_cores)
    for(size_t byte = 0; byte < file_size - MESSAGE_SIZE; byte += MESSAGE_SIZE)
    {

    send: 
        err = send_message(producer, topic, buffer[byte], MESSAGE_SIZE);
        if (err)
        {
            if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL)
            {
                usleep(SHORT_SLEEP);
                goto send;
            }
            fprintf(stderr, "%% Failed to produce to topic %s: %s", topic, rd_kafka_err2str(err));
        }
    }
}

void publish_parallel_for_private(size_t file_size, char* buffer, producer_info_t **producer_infos, int producer_idx, char* topic, int n_cores)
{
    rd_kafka_resp_err_t err;
#pragma omp parallel for num_threads(n_cores)
    for(size_t byte = 0; byte < file_size - MESSAGE_SIZE; byte += MESSAGE_SIZE)
    {
        int my_thread_id = omp_get_thread_num();
        producer_info_t curr_prod_info = producer_infos[my_thread_id][producer_idx];
        rd_kafka_t *curr_producer = curr_prod_info.producer;
        
    send: 
        err = send_message(curr_producer, topic, buffer[byte], MESSAGE_SIZE);
        if (err)
        {
            if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL)
            {
                usleep(SHORT_SLEEP);
                goto send;
            }
            fprintf(stderr, "%% Failed to produce to topic %s: %s", topic, rd_kafka_err2str(err));
        }
    }
}

int get_num_cores()
{
    return sysconf(_SC_NPROCESSORS_ONLN);
}
