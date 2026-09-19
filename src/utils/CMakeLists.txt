file(GLOB_RECURSE UTILS_SOURCES "logger/*.cpp" "thread_pool/*.cpp")

add_library(utils STATIC ${UTILS_SOURCES})

target_include_directories(utils PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

if(UNIX)
    target_link_libraries(utils PUBLIC pthread)
endif()