// hello server: just to check that grpc works (phase 0)
// usage: ./hello_server [address]      default address 0.0.0.0:50051

#include <iostream>
#include <string>
#include <grpcpp/grpcpp.h>
#include "hello.grpc.pb.h"
using namespace std;

// our service: we inherit from the class that protoc generated and fill in the rpc
class GreeterService : public hello::Greeter::Service {
    grpc::Status SayHello(grpc::ServerContext *context, const hello::HelloRequest *request,
                          hello::HelloReply *reply) override {
        reply->set_message("Hello " + request->name());
        return grpc::Status::OK;
    }
};

int main(int argc, char **argv) {
    string address = "0.0.0.0:50051";
    if (argc > 1) address = argv[1];

    GreeterService service;
    grpc::ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    unique_ptr<grpc::Server> server = builder.BuildAndStart();

    cout << "hello server listening on " << address << endl;
    server->Wait();  // runs until the process is killed
    return 0;
}
