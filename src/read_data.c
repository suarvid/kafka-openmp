#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <librdkafka/rdkafka.h>
#include <omp.h>
#include <pthread.h>
#include <sys/sysinfo.h>
#include <string.h>

#include "read_data.h"

ssize_t read_line(FILE *fp, char *linebuf)
{
    size_t len = 0;
    ssize_t read;

    if (fp == NULL)
    {
        fprintf(stderr, "Got null file pointer in read_line!");
        exit(EXIT_FAILURE);
    }

    read = getline(&linebuf, &len, fp);
    return read;
}


// function that gets the number of lines in a file
long count_lines(FILE *fp)
{
    long line_counter = 0;
    char c;
    for (c = getc(fp); c != EOF; c = getc(fp))
    {
        if (c == '\n')
        {
            line_counter = line_counter + 1;
        }
    }

    rewind(fp);
    return line_counter;
}


char *read_num_lines(FILE *fp, long lines_to_read, size_t current_buffer_size, char *read_buffer)
{
    char linebuf[256];
    ssize_t line_size;
    long line_count = 0;
    long write_point = 0; // where in the read_buffer we're going to write

    int accumulated_size = 0;

    while (line_count < lines_to_read)
    {
        line_size = read_line(fp, linebuf);
        if (linebuf[line_size - 1] == '\n')
        {
            linebuf[line_size - 1] == '\0';
        }
        fprintf(stderr, "read line: %s\n");
        if (line_size > (current_buffer_size - accumulated_size))
        {
            current_buffer_size *= 2;
            fprintf(stderr, "reallocing buffer to size: %zu\n", current_buffer_size);
            read_buffer = realloc(read_buffer, current_buffer_size);
        }
        memcpy(read_buffer + write_point, linebuf, line_size);
        write_point += line_size;
        line_count++;
    }

    return read_buffer;
}

struct producer_data
{
    rd_kafka_t *producer;
    char *topic;
    char *data;
    long n_lines;
};


size_t read_line_from_buffer(char *linebuf, char* data_buf)
{
    size_t index = 0;
    while(data_buf[index] != '\n' || data_buf[index] != EOF || data_buf[index] != 10)
    {
        linebuf[index] = data_buf[index];
        fprintf(stderr, "%d char: %c\n", index, data_buf[index]);
        fprintf(stderr, "%d char ascii: %d\n", index, data_buf[index]);
        if (index == 33)
        {
            break;
        }
        
        index++;
    }
    // Feels a bit sketchy
    data_buf += index;

    return index;
}

// function that reads the contents of an entire file into a char buffer
void read_file_contents(FILE *fp, char *buffer, size_t buffer_size)
{
    size_t read_size = fread(buffer, 1, buffer_size, fp);
    if (read_size != buffer_size)
    {
        fprintf(stderr, "Failed to read file\n");
        exit(EXIT_FAILURE);
    }
}

// function which gets the size of a file in bytes
size_t get_file_size(FILE *fp)
{
    fseek(fp, 0L, SEEK_END);
    size_t file_size = ftell(fp);
    rewind(fp);
    return file_size;
}