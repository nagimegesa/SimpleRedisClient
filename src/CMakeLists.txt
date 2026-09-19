add_subdirectory(utils)
add_subdirectory(net)
add_subdirectory(app)

set(PROTOC_SOURCES protoc/hello.pb.cc)

add_executable(redis_cli redis_cli.cpp)
target_link_libraries(redis_cli PRIVATE app_redis net utils)

add_executable(rpc_server rpc_server.cpp ${PROTOC_SOURCES})
target_link_libraries(rpc_server PRIVATE app_rpc net utils)