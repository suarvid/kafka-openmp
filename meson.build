project('parallel-producer', 'c', 'cpp')

cc = meson.get_compiler('c')

c_args = ['-Wall', '-Wextra', '-Ofast', '-lm', '-lrdkafka', '-fopenmp']
c_args_debug = ['-Wall', '-Wextra', '-g', '-lm', '-lrdkafka', '-fopenmp']


inc_dir = include_directories(
)

sources = files(
    'src/main.c',
    'src/read_data.c',
    'src/kafka_utils.c',
    'src/parallel_producer.c',
    'src/producer_builder.c',
    'src/trapezoid.c',
)

kafkadep = dependency('rdkafka')
ompdep = dependency('openmp')
mathdep = cc.find_library('m')

dependencies = [kafkadep, ompdep, mathdep]

executable('parallel-producer',
    sources,
    c_args: c_args,
    include_directories: inc_dir,
    dependencies: dependencies
)