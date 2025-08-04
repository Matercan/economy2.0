cmake_minimum_required(VERSION 3.9.1)
project(economyBot CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")

set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR}/bin)
set(LIBRARY_OUTPUT_PATH ${CMAKE_BINARY_DIR}/lib)

add_executable(economyBot program.cpp)

# Link the C++ filesystem library
target_link_libraries(economyBot PRIVATE stdc++fs) 


target_include_directories(economyBot
    PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/system"
)
