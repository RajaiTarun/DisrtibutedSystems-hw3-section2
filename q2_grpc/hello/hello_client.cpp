// hello client: just to check that grpc works (phase 0)
// usage: ./hello_client [server_address] [name]      default localhost:50051 world

#include <iostream>
#include <string>
#include <grpcpp/grpcpp.h>
#include "hello.grpc.pb.h"
using namespace std;

int main(int argc, char **argv) {
    string address = "localhost:50051";
    string name = "world";
    if (argc > 1) address = argv[1];
    if (argc > 2) name = argv[2];

    // a channel is the connection to the server, the stub is the object we call the rpcs on
    auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    unique_ptr<hello::Greeter::Stub> stub = hello::Greeter::NewStub(channel);

    hello::HelloRequest request;
    request.set_name(name);
    hello::HelloReply reply;
    grpc::ClientContext context;

    grpc::Status status = stub->SayHello(&context, request, &reply);
    if (!status.ok()) {
        cout << "rpc failed: " << status.error_message() << endl;
        return 1;
    }
    cout << reply.message() << endl;
    return 0;
}
