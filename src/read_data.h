#pragma once
#include <librdkafka/rdkafka.h>
#include <stdio.h>
#include <stdlib.h>

size_t read_line_from_buffer(char *linebuf, char* data_buf);
char *read_num_lines(FILE *fp, long lines_to_read, size_t current_buffer_size, char *read_buffer);
long count_lines(FILE *fp);
ssize_t read_line(FILE *fp, char *linebuf);
void read_file_contents(FILE *fp, char *buffer, size_t buffer_size);
size_t get_file_size(FILE *fp);