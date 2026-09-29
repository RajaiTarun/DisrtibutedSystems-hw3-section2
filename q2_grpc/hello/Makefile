# builds the hello test (phase 0): checks that protoc, the grpc plugin and the grpc libraries work
# usage: make        then: ./hello_server &   ./hello_client

CXX = g++
# -Wno-deprecated-declarations hides warnings that come from inside the grpc headers (not our code)
CXXFLAGS = -std=c++17 -O2 -Wno-deprecated-declarations $(shell pkg-config --cflags grpc++ protobuf)
LDFLAGS = $(shell pkg-config --libs grpc++ protobuf)
GRPC_PLUGIN = $(shell which grpc_cpp_plugin)

all: hello_server hello_client

# protoc generates the message classes (hello.pb.*) and the service classes (hello.grpc.pb.*)
hello.pb.cc hello.grpc.pb.cc: hello.proto
	protoc --cpp_out=. --grpc_out=. --plugin=protoc-gen-grpc=$(GRPC_PLUGIN) hello.proto

hello_server: hello_server.cpp hello.pb.cc hello.grpc.pb.cc
	@echo "compiling $@"
	@$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

hello_client: hello_client.cpp hello.pb.cc hello.grpc.pb.cc
	@echo "compiling $@"
	@$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

clean:
	rm -f hello_server hello_client hello.pb.* hello.grpc.pb.*
