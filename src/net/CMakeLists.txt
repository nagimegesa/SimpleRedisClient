file(GLOB_RECURSE NET_SOURCES "context/*.cpp" "socket/*.cpp")

find_library(TCMALLOC_LIB tcmalloc REQUIRED PATHS /usr/bin/lib)

if(WIN32)
    list(REMOVE_ITEM NET_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/socket/LinuxSocket.cpp")
elseif(UNIX)
    list(REMOVE_ITEM NET_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/socket/WindowsSocket.cpp")
endif()

add_library(net STATIC ${NET_SOURCES})
target_link_libraries(net utils ${TCMALLOC_LIB} pthread)

target_include_directories(net PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})