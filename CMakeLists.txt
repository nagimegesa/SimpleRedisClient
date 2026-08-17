cmake_minimum_required(VERSION 3.10)

project(Demo)
set(CMAKE_CXX_STANDARD 20)


add_library(
        socket
        src/socket/SocketManager.cpp
        src/socket/WindowsSocket.cpp
)
target_link_libraries(
        socket
        ws2_32   # 添加这一行
)
target_include_directories(
        socket PUBLIC
        src/socket/
)

add_library(utils INTERFACE)
target_include_directories(utils INTERFACE src/utils)

add_library(thread src/thread_pool/thread_pool.cpp)
target_include_directories(thread PUBLIC src/thread_pool/)

add_library(logger src/logger/logger.cpp)
target_include_directories(logger PUBLIC src/logger)
target_link_libraries(logger utils)

add_executable(main src/main.cpp)

add_executable(test_thread_pool test/test_thread_pool_benchmark.cpp)
add_executable(test_thread_pool_result test/test_thread_pool_result.cpp)

target_link_libraries(main thread logger socket)


target_link_libraries(test_thread_pool thread)
target_link_libraries(test_thread_pool_result thread)
target_include_directories(test_thread_pool PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_include_directories(test_thread_pool_result PUBLIC ${PROJECT_SOURCE_DIR}/src)