cmake_minimum_required(VERSION 2.8)
project(ASM)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
add_compile_options(-std=c++11)

set(SOURCE_FILES
        Protocols/sm_protocol.hpp
        Protocols/sm_protocol.cpp

        Protocols/ABD/abd_client.cpp
        Protocols/ABD/abd_client.hpp
        Protocols/ABD/abd_server.cpp
        Protocols/ABD/abd_server.hpp

        Protocols/CCHYBRID/cchybrid_client.cpp
        Protocols/CCHYBRID/cchybrid_client.hpp
        Protocols/CCHYBRID/cchybrid_server.cpp
        Protocols/CCHYBRID/cchybrid_server.hpp

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

add_executable(asm ${SOURCE_FILES})

FIND_LIBRARY(ZMQ_LIB zmq-static)
TARGET_LINK_LIBRARIES(asm ${ZMQ_LIB})
