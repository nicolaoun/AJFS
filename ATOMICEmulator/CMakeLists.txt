cmake_minimum_required(VERSION 2.8)
project(ASM)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
add_compile_options(-std=c++11)

set(SOURCE_FILES
        Protocols/ABD/abd_protocol.hpp
        Protocols/ABD/abd_protocol.cpp
        Protocols/ABD/abd_client.cpp
        Protocols/ABD/abd_client.hpp
        Protocols/ABD/abd_server.cpp
        Protocols/ABD/abd_server.hpp

        Utilities/set_operations.hpp
        Utilities/set_operations.cpp
        Utilities/sm_common.cpp
        Utilities/sm_common.h
        Utilities/sm_node.cpp
        Utilities/sm_node.h
        Utilities/arguments.cpp
        Utilities/arguments.h
        Utilities/filesystem.h
        Utilities/filesystem.cpp

        main.cpp
        )

include_directories(${CMAKE_SOURCE_DIR}/Protocols)
include_directories(${CMAKE_SOURCE_DIR}/Utilities)
include_directories(/usr/local/lib)
TARGET_LINK_LIBRARIES(zmq-static)

add_executable(asm ${SOURCE_FILES})
