cmake_minimum_required(VERSION 3.10)

set(CMAKE_C_COMPILER /usr/bin/gcc-14 CACHE PATH "C compiler")
set(CMAKE_CXX_COMPILER /usr/bin/g++-14 CACHE PATH "C++ compiler")

project(Demo)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_subdirectory(src)

add_executable(test_thread_pool test/test_thread_pool/test_thread_pool_benchmark.cpp)
add_executable(test_thread_pool_result test/test_thread_pool/test_thread_pool_result.cpp)
add_executable(test_redis_acc test/test_redis/test_redis_parser.cpp)
add_executable(test_redis_bench test/test_redis/test_redis_bench.cpp)
add_executable(test_redis_client_acc test/test_redis/test_redis_client_bench.cpp)
add_executable(test_spsc_queue test/test_queue/test_spsc_queue.cpp)
add_executable(test_echo_server test/test_net/test_echo_server.cpp)
add_executable(test_nacos test/test_nacos/test_nacos.cpp)

target_link_libraries(test_thread_pool utils)
target_link_libraries(test_thread_pool_result utils)
target_link_libraries(test_redis_acc app_redis)
target_link_libraries(test_redis_client_acc app_redis)
target_link_libraries(test_redis_bench app_redis)
target_link_libraries(test_spsc_queue utils)
target_link_libraries(test_echo_server utils net)
target_link_libraries(test_nacos app_rpc)