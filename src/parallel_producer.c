#include <stdio.h>
#include <librdkafka/rdkafka.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include <omp.h>

#include "read_data.h"
#include "kafka_utils.h"
#include "parallel_producer.h"
#include "producer_builder.h"

int get_num_cores();
pthread_t *create_threads(int n_threads, void *thread_func(void *), void *args);
void thread_process_data(void *args);

void publish_with_n_cores(FILE *fp, char *broker, char *topic, int n_cores)
{
    int max_cores = get_num_cores();
    if (n_cores > max_cores)
    {
        fprintf(stderr, "Number of cores requested is greater than the number of cores available. Using %d cores instead.\n", max_cores);
        n_cores = max_cores;
    }
    fprintf(stderr, "Using %d cores\n", n_cores);

    size_t file_size = get_file_size(fp);
    char *buffer = malloc(file_size);

    size_t bytes_per_thread = file_size / n_cores;
    int messages_per_thread = bytes_per_thread / (n_cores * MESSAGE_SIZE);

    read_file_contents(fp, buffer, file_size);

    struct thread_args *default_args = malloc(sizeof(struct thread_args));
    default_args->buffer = buffer;
    default_args->messages_to_read = messages_per_thread;
    default_args->topic = topic;
    default_args->broker = broker;

    pthread_t *threads = create_threads(n_cores, thread_process_data, default_args);

    for (int i = 0; i < n_cores; i++)
    {
        pthread_join(threads[i], NULL);
    }

    free(buffer);
}

static void omp_thread_process_data_shared_producer(struct omp_thread_args_shared_producer args)
{
    int my_thread_num = omp_get_thread_num();
    fprintf(stderr, "Starting data processing on thread %d\n", my_thread_num);
    int start_point = my_thread_num * args.messages_per_thread;
    rd_kafka_resp_err_t err;

    for (int i = 0; i < args.messages_per_thread; i++)
    {
        int message_index = start_point + i;
        char *message = args.buffer + (message_index * MESSAGE_SIZE);
    send:
        err = send_message(args.producer, args.topic, message, MESSAGE_SIZE);
        if (err)
        {
            if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL)
            {
                sleep(1);
                goto send;
            }
            fprintf(stderr, "%% Failed to produce to topic %s: %s", args.topic, rd_kafka_err2str(err));
        }
    }
}

size_t publish_with_omp_shared_producer(const FILE *fp, const char *brokers, const char *topic, int n_threads)
{
    size_t file_size = get_file_size(fp);
    char *buffer = malloc(file_size);
    read_file_contents(fp, buffer, file_size);

    fprintf(stderr, "Using %d threads\n", n_threads);


    struct omp_thread_args_shared_producer args;
    args.broker = brokers;
    args.topic = topic;
    args.messages_per_thread = file_size / (n_threads * MESSAGE_SIZE);
    args.buffer = buffer;
    args.producer = create_producer_high_throughput_no_acks_no_idemp_lz4(brokers);
    //args.producer = create_producer_high_throughput_one_ack_no_idemp_snappy(brokers);
    //args.producer = create_producer_low_latency_acks_one_no_idemp(brokers);
#   pragma omp parallel num_threads(n_threads)
    omp_thread_process_data_shared_producer(args);
#   pragma omp barrier
    free(buffer);
    return file_size;

}

static void omp_thread_process_data_private_producer(struct omp_thread_args_private_producer args)
{
    int my_thread_num = omp_get_thread_num();
    fprintf(stderr, "Starting data processing on thread %d\n", my_thread_num);
    int start_point = my_thread_num * args.messages_per_thread;
    //rd_kafka_t *producer = create_producer_high_throughput_no_acks_no_idemp_snappy(args.broker);
    rd_kafka_t *producer = create_producer_low_latency_no_acks_no_idemp(args.broker);
    rd_kafka_resp_err_t err;

    for (int i = 0; i < args.messages_per_thread; i++)
    {
        int message_index = start_point + i;
        char *message = args.buffer + (message_index * MESSAGE_SIZE);
    send:
        err = send_message(producer, args.topic, message, MESSAGE_SIZE);
        if (err)
        {
            if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL)
            {
                sleep(1);
                goto send;
            }
            fprintf(stderr, "%% Failed to produce to topic %s: %s", args.topic, rd_kafka_err2str(err));
        }
    }

    flush_destroy_producer(producer);
}

size_t publish_with_omp_private_producer(const FILE *fp, const char *brokers, const char *topic, int n_threads)
{
    size_t file_size = get_file_size(fp);
    char *buffer = malloc(file_size);
    read_file_contents(fp, buffer, file_size);

    fprintf(stderr, "Using %d threads\n", n_threads);


    struct omp_thread_args_private_producer args;
    args.broker = brokers;
    args.topic = topic;
    args.messages_per_thread = file_size / (n_threads * MESSAGE_SIZE);
    args.buffer = buffer;
#   pragma omp parallel num_threads(n_threads)
    omp_thread_process_data_private_producer(args);
#   pragma omp barrier
    free(buffer);
    return file_size;
}





int get_num_cores()
{
    return sysconf(_SC_NPROCESSORS_ONLN);
}

// function that creates a specified number of threads
// and starts them running a specified function with a specified argument
pthread_t *create_threads(int n_threads, void *thread_func(void *), void *args)
{
    fprintf(stderr, "Creating threads\n");
    struct thread_args *default_args = (struct thread_args *)args;
    pthread_t *threads = malloc(sizeof(pthread_t) * n_threads);
    int i;
    for (i = 0; i < n_threads; i++)
    {
        // Each thread needs to calculate its own start point
        struct thread_args *my_args = malloc(sizeof(struct thread_args));
        memcpy(my_args, default_args, sizeof(struct thread_args));
        my_args->start_point = i * (default_args->messages_to_read);

        pthread_create(&threads[i], NULL, thread_func, my_args);
    }

    return threads;
}

void thread_process_data(void *args)
{
    fprintf(stderr, "Starting thread\n");
    struct thread_args *t_args = (struct thread_args *)args;
    int message_count = 0;
    char message_buf[MESSAGE_SIZE];
    char *buffer = t_args->buffer;
    // rd_kafka_t *producer = create_producer_basic(t_args->broker);
    rd_kafka_t *producer = create_producer_ack_one(t_args->broker);
    while (message_count < t_args->messages_to_read)
    {
        memcpy(message_buf, buffer + (message_count * MESSAGE_SIZE) + t_args->start_point, MESSAGE_SIZE);
        rd_kafka_resp_err_t err = send_message(producer, t_args->topic, message_buf, MESSAGE_SIZE);
        if (err)
        {
            fprintf(stderr, "Failed to send message: %s\n", rd_kafka_err2str(err));
        }
        else
        {
            message_count++;
        }
    }

    flush_destroy_producer(producer);
}