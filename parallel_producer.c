#include <stdio.h>
#include <librdkafka/rdkafka.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>

#include "read_data.h"
#include "kafka_utils.h"
#include "parallel_producer.h"

int get_num_cores();
pthread_t *create_threads(int n_threads, void *thread_func(void *), void *args);
void thread_process_data(void *args);

void publish_in_parallel(FILE *fp, char *broker, char *topic)
{
    // Keep it simple: we know each message is exactly 32 bytes
    // So what do we do?
    // We get the number of lines in the file and the number of threads
    // Then we divide the number of lines by the number of threads
    // Then we read that many lines into a buffer
    // Then we start a producer in each thread
    // Then we send the buffer to the producer
    int n_cores = get_num_cores();
    fprintf(stderr, "Fetching number of lines...\n");
    int n_lines = count_lines(fp);
    fprintf(stderr, "Number of lines: %d\n", n_lines);
    int lines_per_thread = n_lines / n_cores;
    int messages_per_thread = lines_per_thread / MESSAGE_SIZE;
    // just read the entire file contents
    // we know how many lines it contains, each one is 32 bytes
    // and how many threads there are
    // so we can just read the entire file into a buffer
    // and then send that buffer to each thread
    // along with where in the buffer to start reading
    // and how many lines to read
    // then read 32 bytes at a time from the buffer
    // and send that to the producer
    size_t buffer_size = n_lines * MESSAGE_SIZE;
    char *buffer = malloc(buffer_size);
    read_file_contents(fp, buffer, buffer_size);

    struct thread_args *default_args = malloc(sizeof(struct thread_args));
    default_args->buffer = buffer;
    default_args->messages_to_read = messages_per_thread;
    default_args->topic = topic;
    default_args->broker = broker;

    pthread_t * threads = create_threads(n_cores, thread_process_data, default_args);

    for (int i = 0; i < n_cores; i++)
    {
        pthread_join(threads[i], NULL);
    }

    
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
    struct thread_args *default_args = (struct thread_args *) args;
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
    struct thread_args *t_args = (struct thread_args *) args;
    // args should be a struct with the following fields:
    // char *buffer
    // int start_line
    // int lines_to_read
    // char *topic
    // char *broker
    // char *errstr
    // int part
    // might actually need to pass in a producer
    // or make a producer per thread?
    int message_count = 0;
    char message_buf[MESSAGE_SIZE];
    char *buffer = t_args->buffer;
    rd_kafka_t *producer = create_producer_basic(t_args->broker);
    while (message_count < t_args->messages_to_read)
    {
        memcpy(message_buf, buffer + (message_count * MESSAGE_SIZE) + t_args->start_point, MESSAGE_SIZE);
        rd_kafka_resp_err_t err = send_message(producer, t_args->topic, message_buf, MESSAGE_SIZE);
        if (err)
        {
            fprintf("Failed to send message: %s\n", rd_kafka_err2str(err));
        } else {
            message_count++;
        }
    }

    flush_destroy_producer(producer);
}