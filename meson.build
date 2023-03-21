project('simple-producer', 'c', 'cpp')

cc = meson.get_compiler('c')

c_args = ['-Wall', '-Wextra', '-g', '-lrdkafka']


inc_dir = include_directories(
)

sources = files(
    'main.c',
    'read_data.c',
    'kafka_utils.c',
    'parallel_producer.c'
)

kafkadep = dependency('rdkafka')

executable('simple-producer',
    sources,
    c_args: c_args,
    include_directories: inc_dir,
    dependencies: kafkadep
)