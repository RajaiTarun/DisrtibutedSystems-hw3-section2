# builds the hello test (phase 0): checks that protoc, the grpc plugin and the grpc libraries work
# usage: make        then: ./hello_server &   ./hello_client
#
# on the rce cluster run "module load gRPC/1.74.1" first. that module sets GRPC_HOME, but its
# PKG_CONFIG_PATH only points to lib64/pkgconfig, so we also add lib/pkgconfig ourselves
# (on the mac GRPC_HOME is not set and pkg-config already finds the homebrew grpc)

CXX = g++

PC_PATH = $(GRPC_HOME)/lib/pkgconfig:$(GRPC_HOME)/lib64/pkgconfig:$(PKG_CONFIG_PATH)
PKG = PKG_CONFIG_PATH=$(PC_PATH) pkg-config

# -Wno-deprecated-declarations hides warnings that come from inside the grpc headers (not our code)
CXXFLAGS = -std=c++17 -O2 -Wno-deprecated-declarations $(shell $(PKG) --cflags grpc++ protobuf)
LDFLAGS = $(shell $(PKG) --libs grpc++ protobuf)
GRPC_PLUGIN = $(shell which grpc_cpp_plugin)

all: hello_server hello_client

# prints what make found, useful when the build fails on a new machine
check:
	@echo "grpc++   : $$($(PKG) --modversion grpc++)"
	@echo "protobuf : $$($(PKG) --modversion protobuf)"
	@echo "protoc   : $$(which protoc) ($$(protoc --version))"
	@echo "plugin   : $(GRPC_PLUGIN)"
	@echo "g++      : $$($(CXX) --version | head -1)"

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

.PHONY: all check clean
