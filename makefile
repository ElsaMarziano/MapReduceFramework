cmake_minimum_required(VERSION 3.10)

# Set the project name
project(MapReduceFramework)

# Specify the C++ standard
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Add the executable
add_executable(sampleclient
    MapReduceFramework.cpp
    JobContext.cpp
    SampleClient.cpp
    Barrier.cpp
)

# Add the header files
target_include_directories(sampleclient PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# Link pthread library
target_link_libraries(sampleclient pthread)
