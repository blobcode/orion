#!/bin/bash
# Build script for the distributed Lua tasks system

# Generate C++ code from Protocol Buffers definition
protoc --cpp_out=. task.proto

# Compile the C++ program with required libraries
g++ -std=c++11 -o orion main.cpp task.pb.cc \
    -lprotobuf -levent -llua5.4 -ldl

# Example usage:
# Start server: ./orion server 8888
# Run client: ./orion client localhost 8888 example_task.lua "1,2,3,4,5"
