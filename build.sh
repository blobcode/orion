mkdir -p build 
protoc --cpp_out=build/ src/task.proto -Isrc
g++ -Ibuild src/main.cpp build/task.pb.cc -o build/orion -luv -lm -ldl -llua5.4 -lprotobuf