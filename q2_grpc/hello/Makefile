# builds the hello test (phase 0): checks that protoc, the grpc plugin and the grpc libraries work
# usage: make        then: ./hello_server &   ./hello_client
#
# mac:     pkg-config finds the homebrew grpc (shared libraries), nothing special needed
# cluster: run "module load gRPC/1.74.1" first (it sets GRPC_HOME). that module has only static
#          libraries (.a files) and pkg-config does not work for it, so we link all the .a files
#          of the module in one "group": inside a group the linker keeps searching all the files
#          until everything is found, so we do not need to know the right order ourselves

CXX = g++
GRPC_PLUGIN = $(shell which grpc_cpp_plugin)

ifdef GRPC_HOME
# ---------------- cluster (static libraries from the gRPC module) ----------------
# every static library of the module, except the "unsecure" variants (they duplicate libgrpc)
GRPC_LIBS = $(filter-out %unsecure.a, $(wildcard $(GRPC_HOME)/lib/*.a $(GRPC_HOME)/lib64/*.a))

# system libraries: only add openssl / zlib from the system if the module does not have its own copy
SYSTEM_LIBS = -lpthread -ldl -lrt
ifeq ($(wildcard $(GRPC_HOME)/lib/libssl.a $(GRPC_HOME)/lib64/libssl.a),)
SYSTEM_LIBS += -lssl -lcrypto
endif
ifeq ($(wildcard $(GRPC_HOME)/lib/libz.a $(GRPC_HOME)/lib64/libz.a),)
SYSTEM_LIBS += -lz
endif
# libsystemd: use the .so file directly (the short -lsystemd name needs a dev package)
SYSTEMD = $(firstword $(wildcard /usr/lib64/libsystemd.so /usr/lib64/libsystemd.so.0))

CXXFLAGS = -std=c++17 -O2 -Wno-deprecated-declarations -I$(GRPC_HOME)/include
LDFLAGS = -Wl,--start-group $(GRPC_LIBS) -Wl,--end-group $(SYSTEMD) $(SYSTEM_LIBS)
else
# ---------------- mac (homebrew, shared libraries) ----------------
# -Wno-deprecated-declarations hides warnings that come from inside the grpc headers (not our code)
CXXFLAGS = -std=c++17 -O2 -Wno-deprecated-declarations $(shell pkg-config --cflags grpc++ protobuf)
LDFLAGS = $(shell pkg-config --libs grpc++ protobuf)
endif

all: hello_server hello_client

# prints what make found, useful when the build fails on a new machine
check:
	@echo "protoc      : $$(which protoc) ($$(protoc --version))"
	@echo "plugin      : $(GRPC_PLUGIN)"
	@echo "g++         : $$($(CXX) --version | head -1)"
	@echo "GRPC_HOME   : $(GRPC_HOME)"
	@echo "static libs : $(words $(GRPC_LIBS)) files"
	@echo "systemd     : $(SYSTEMD)"
	@echo "system libs : $(SYSTEM_LIBS)"

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
