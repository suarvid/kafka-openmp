#!/bin/bash
builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 1 t 100 &
wait; builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 2 t 100 &
wait; builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 6 t 100 &
wait; builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 12 t 100 &
wait; builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 24 t 100 &
wait; builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 1 t 1000 &
wait; builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 2 t 1000 &
wait; builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 6 t 1000 &
wait; builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 12 t 1000 &
wait; builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 24 t 1000 &
wait; builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 1 t 10000 &
wait; builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 2 t 10000 &
wait; builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 6 t 10000 &
wait; builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 12 t 10000 &
wait; builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 24 t 10000