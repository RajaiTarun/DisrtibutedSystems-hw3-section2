#!/bin/bash
# creates the python virtual environment .venv (inside this folder) for Q2
#
# cluster (default):  only grpcio + protobuf (requirements.txt). if the chosen python already has
#                     them, NOTHING is installed: the venv just uses the python's own packages
# laptop  (--laptop): also grpcio-tools (to regenerate the gRPC code) and pandas + matplotlib (plots)
#
# grpcio needs python 3.9 or newer (the cluster's default python3 is 3.6, which does not work), so the
# newest python found is used, or the one given with PYTHON=...
# everything goes into .venv in this folder; "rm -rf .venv" removes it again.
#
# usage:
#   PYTHON=/usr/bin/python3.11 ./setup_env.sh        (cluster)
#   module load python/3.12.5 && ./setup_env.sh      (cluster, with the python module)
#   ./setup_env.sh --laptop                          (mac / laptop)

cd "$(dirname "$0")"
MODE=${1:-cluster}

if [ -z "$PYTHON" ]; then
    for candidate in python3.13 python3.12 python3.11 python3.10 python3.9 python3; do
        if command -v "$candidate" > /dev/null &&
           "$candidate" -c "import sys; sys.exit(0 if sys.version_info >= (3, 9) else 1)" 2> /dev/null; then
            PYTHON=$(command -v "$candidate")
            break
        fi
    done
fi
if [ -z "$PYTHON" ]; then
    echo "no python 3.9 or newer found. on the cluster: PYTHON=/usr/bin/python3.11 ./setup_env.sh"
    exit 1
fi
echo "using $PYTHON ($("$PYTHON" --version))"

# does this python already have grpcio + a protobuf new enough for our generated code?
if [ "$MODE" != "--laptop" ] && "$PYTHON" -c "import grpc, weather_pb2_grpc" 2> /dev/null; then
    # yes: a venv that sees the python's own packages, and nothing to install
    echo "this python already has grpcio and protobuf, nothing will be installed"
    "$PYTHON" -m venv --system-site-packages .venv || exit 1
else
    "$PYTHON" -m venv .venv || exit 1
    if [ "$MODE" = "--laptop" ]; then
        .venv/bin/python -m pip install -r requirements-laptop.txt || exit 1
    else
        .venv/bin/python -m pip install -r requirements.txt || exit 1
    fi
fi

# weather_pb2.py / weather_pb2_grpc.py are in the repository; they only need to be regenerated
# (with grpcio-tools, laptop only) after weather.proto changes
if [ "$MODE" = "--laptop" ]; then
    PATH="$(pwd)/.venv/bin:$PATH" ./gen_proto.sh || exit 1
fi

.venv/bin/python -c "import grpc, weather_pb2_grpc; print('grpcio', grpc.__version__, 'ok')" || exit 1
echo ""
echo "done. the scripts use .venv/bin/python directly; to run programs by hand: source .venv/bin/activate"
