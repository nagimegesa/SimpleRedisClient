cmake_minimum_required(VERSION 3.10)

set(CMAKE_C_COMPILER /usr/bin/gcc-14 CACHE PATH "C compiler")
set(CMAKE_CXX_COMPILER /usr/bin/g++-14 CACHE PATH "C++ compiler")

project(Demo)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(utils src/utils/logger/Logger.cpp)
target_include_directories(utils PUBLIC src/utils)

add_library(thread
        src/utils/thread_pool/thread_pool.cpp
        src/utils/thread_pool/lock/SpinLock.cpp
)

target_link_libraries(
        thread
        utils
)

if (CMAKE_SYSTEM_NAME MATCHES "Linux")
    target_link_libraries(thread pthread)
endif()

target_include_directories(thread PUBLIC src/utils/thread_pool/)

add_library(
        net
        src/net/socket/SocketManager.cpp
        src/net/socket/WindowsSocket.cpp
        src/net/socket/LinuxSocket.cpp
        src/net/context/EpollContext.cpp
)

target_link_libraries(
        net
        utils
        thread
)

target_include_directories(
        net PUBLIC
        src/net/
)

add_library(redis src/app/SimpleRedisClient.cpp)
target_link_libraries(redis net)
target_include_directories(redis PUBLIC src/app)

if (CMAKE_SYSTEM_NAME MATCHES "Windows")
    target_link_libraries(socket ws_sc2)
endif()

add_executable(main src/main.cpp)
add_executable(redis_cli src/redis_cli.cpp)

add_executable(test_thread_pool test/test_thread_pool_benchmark.cpp)
add_executable(test_thread_pool_result test/test_thread_pool_result.cpp)
add_executable(test_redis_acc test/test_redis_parser.cpp)
add_executable(test_redis_bench test/test_redis_bench.cpp)
add_executable(test_redis_client_acc test/test_redis_client_bench.cpp)

target_link_libraries(main thread utils net)
target_link_libraries(redis_cli redis)

target_link_libraries(test_thread_pool thread)
target_link_libraries(test_thread_pool_result thread)
target_link_libraries(test_redis_acc redis)
target_link_libraries(test_redis_client_acc redis)
target_link_libraries(test_redis_bench redis)
target_include_directories(test_thread_pool PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_include_directories(test_thread_pool_result PUBLIC ${PROJECT_SOURCE_DIR}/src)