#!/bin/bash
# generates the python gRPC code from weather.proto:
#   weather_pb2.py       the message classes (Record, RecordBatch, ...)
#   weather_pb2_grpc.py  the service classes (servicer base classes + client stubs)
# run it again whenever weather.proto changes (setup_env.sh runs it once)

cd "$(dirname "$0")"
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. weather.proto && echo "generated weather_pb2.py and weather_pb2_grpc.py"
