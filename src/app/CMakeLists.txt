file(GLOB_RECURSE APP_REDIS_SOURCES "redis/*.cpp")
file(GLOB_RECURSE APP_RPC_SOURCES CONFIGURE_DEPENDS "rpc/*.cpp" "rpc/*.h")

add_library(app_redis STATIC ${APP_REDIS_SOURCES})
target_include_directories(app_redis PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(app_redis PUBLIC net utils)

add_library(app_rpc STATIC ${APP_RPC_SOURCES})
target_include_directories(app_rpc PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(app_rpc PUBLIC ${CMAKE_SOURCE_DIR}/3rdparty/nacos-sdk-cpp/include)
target_link_libraries(app_rpc PUBLIC net utils)

if (${CMAKE_CXX_COMPILER_ID} STREQUAL "GNU")
    target_compile_options(app_rpc PRIVATE -fcoroutines)
endif ()

target_link_libraries(app_rpc PUBLIC
        ${CMAKE_SOURCE_DIR}/3rdparty/nacos-sdk-cpp/libnacos-cli-static.a
        curl
        z
        protobuf
        pthread
)