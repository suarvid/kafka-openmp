project('parallel-producer', 'c', 'cpp')

cc = meson.get_compiler('c')

c_args = ['-Wall', '-Wextra', '-Ofast', '-lrdkafka', '-fopenmp']
c_args_debug = ['-Wall', '-Wextra', '-g', '-lrdkafka', '-fopenmp']


inc_dir = include_directories(
)

sources = files(
    'src/main.c',
    'src/read_data.c',
    'src/kafka_utils.c',
    'src/parallel_producer.c'
)

kafkadep = dependency('rdkafka')
ompdep = dependency('openmp')

dependencies = [kafkadep, ompdep]

executable('parallel-producer',
    sources,
    c_args: c_args,
    include_directories: inc_dir,
    dependencies: dependencies
)