#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

int main(void)
{
    FILE *fp = fopen("text_data.txt", "r");
    if (fp == NULL)
    {
        fprintf(stderr, "FAILED TO OPEN FILE!\n");
        return EXIT_FAILURE;
    }
    char linebuf[256];
    ssize_t bytes_read = getline(&linebuf, fp);
    
}