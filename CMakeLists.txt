cmake_minimum_required(VERSION 3.22)
project(to_infiniband_and_beyond)

set(CMAKE_CXX_STANDARD_REQUIRED 20)
set(CMAKE_CXX_FLAGS "-std=c++20 -Wno-narrowing")

include_directories("./")

link_libraries("ibverbs")

add_executable(to_infiniband_and_beyond "main.cpp")
