#!/bin/bash
builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 1 t 50 &
wait; builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 2 t 50 &
wait; builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 6 t 50 &
wait; builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 12 t 50 &
wait; builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 24 t 50 &
wait; builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 1 t 500 &
wait; builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 2 t 500 &
wait; builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 6 t 500 &
wait; builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 12 t 500 &
wait; builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 24 t 500 &
wait; builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 1 t 2000 &
wait; builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 2 t 2000 &
wait; builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 6 t 2000 &
wait; builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 12 t 2000 &
wait; builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 24 t 2000 &
wait; builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 1 t 5000 &
wait; builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 2 t 5000 &
wait; builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 6 t 5000 &
wait; builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 12 t 5000 &
wait; builddir/parallel-producer 192.168.0.131:9092 benchmark input_data/real_10GB.bin 24 t 5000 