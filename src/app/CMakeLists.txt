file(GLOB_RECURSE APP_REDIS_SOURCES "redis/*.cpp")
file(GLOB_RECURSE APP_RPC_SOURCES "rpc/*.cpp")

add_library(app_redis STATIC ${APP_REDIS_SOURCES})
target_include_directories(app_redis PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(app_redis PUBLIC net utils)

add_library(app_rpc STATIC ${APP_RPC_SOURCES})
target_include_directories(app_rpc PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(app_rpc PRIVATE ${CMAKE_SOURCE_DIR}/3rdparty/nacos-sdk-cpp/include)
target_link_libraries(app_rpc PUBLIC net utils)

target_link_libraries(app_rpc PUBLIC
        ${CMAKE_SOURCE_DIR}/3rdparty/nacos-sdk-cpp/libnacos-cli-static.a
        curl
        z
        protobuf
)