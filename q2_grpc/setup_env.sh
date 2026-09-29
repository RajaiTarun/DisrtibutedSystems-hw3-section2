#!/bin/bash
# creates the python virtual environment .venv (inside this folder) with the packages from
# requirements.txt, and generates the gRPC code from weather.proto
#
# grpcio needs python 3.9 or newer (the cluster's default python3 is 3.6, which does not work),
# so this script picks the newest python it can find, or the one given with PYTHON=...
#
# usage:
#   ./setup_env.sh                      (picks a python automatically)
#   PYTHON=/usr/bin/python3.11 ./setup_env.sh
# afterwards, in every new terminal:
#   source .venv/bin/activate

cd "$(dirname "$0")"

if [ -z "$PYTHON" ]; then
    for candidate in python3.13 python3.12 python3.11 python3.10 python3.9 python3; do
        if command -v "$candidate" > /dev/null; then
            # check the version really is 3.9 or newer
            if "$candidate" -c "import sys; sys.exit(0 if sys.version_info >= (3, 9) else 1)"; then
                PYTHON=$(command -v "$candidate")
                break
            fi
        fi
    done
fi
if [ -z "$PYTHON" ]; then
    echo "no python 3.9 or newer found. on the cluster: module load python/3.12.5 (or use /usr/bin/python3.11)"
    exit 1
fi

echo "using $PYTHON ($("$PYTHON" --version))"
"$PYTHON" -m venv .venv || exit 1
source .venv/bin/activate
python -m pip install --upgrade pip > /dev/null
python -m pip install -r requirements.txt || exit 1

./gen_proto.sh || exit 1
python -c "import grpc; print('grpcio', grpc.__version__, 'ok')"
echo ""
echo "done. in every new terminal run:   source .venv/bin/activate"
