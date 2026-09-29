#!/bin/bash
# starts / stops the system on the RCE cluster for a live demo (like the RCE gRPC execution guide):
# coordinator on the first allocated node, workers on the other nodes, and it prints the commands to
# start the client and the dashboards on other nodes (each in its own terminal with ssh)
#
# usage (from inside q2_grpc, on the login node):
#   ./setup_env.sh                                    (once)
#   salloc --nodes=4 --ntasks-per-node=1 --time=01:00:00
#   ./run_cluster.sh start [workers] [strategy]       (default 4 workers, interval)
#   ... run the printed client / dashboard commands in other terminals ...
#   ./run_cluster.sh stop
#   exit                                              (gives the nodes back)

cd "$(dirname "$0")"
PY=$(pwd)/.venv/bin/python
ACTION=$1
W=${2:-4}
STRATEGY=${3:-interval}
BASE_PORT=${BASE_PORT:-$((40000 + $(id -u) % 20000))}
STATE=.cluster_coordinator

if [ ! -x "$PY" ]; then
    echo "run ./setup_env.sh first"
    exit 1
fi
[ -f weather_pb2.py ] || ./gen_proto.sh > /dev/null

if [ -z "$SLURM_JOB_NODELIST" ]; then
    echo "run this inside an allocation: salloc --nodes=4 --ntasks-per-node=1 --time=01:00:00"
    exit 1
fi
NODES=($(scontrol show hostnames "$SLURM_JOB_NODELIST"))
NUM_NODES=${#NODES[@]}

if [ "$ACTION" = "stop" ]; then
    if [ -f "$STATE" ]; then
        GRPC_CONNECT_TIMEOUT=5 "$PY" dashboard.py "$(cat "$STATE")" --shutdown && echo "stopped coordinator and workers"
        rm -f "$STATE"
    else
        echo "nothing to stop"
    fi
    exit 0
fi
if [ "$ACTION" != "start" ]; then
    echo "usage: ./run_cluster.sh start [workers] [strategy]   |   ./run_cluster.sh stop"
    exit 1
fi
mkdir -p logs

COORD_NODE=${NODES[0]}
# workers on every node except the first one (or on the first one if there is only one node)
if [ "$NUM_NODES" -ge 2 ]; then
    WORKER_NODES=("${NODES[@]:1}")
else
    WORKER_NODES=("${NODES[0]}")
fi

ADDRESSES=()
for ((i = 0; i < W; i++)); do
    NODE=${WORKER_NODES[$((i % ${#WORKER_NODES[@]}))]}
    PORT=$((BASE_PORT + 1 + i))
    srun --overlap -N1 -n1 -w "$NODE" "$PY" worker.py "0.0.0.0:$PORT" > "logs/worker_$i.log" 2>&1 &
    ADDRESSES+=("$NODE:$PORT")
done
srun --overlap -N1 -n1 -w "$COORD_NODE" "$PY" coordinator.py "0.0.0.0:$BASE_PORT" "$STRATEGY" "${ADDRESSES[@]}" \
    > logs/coordinator.log 2>&1 &
COORD="$COORD_NODE:$BASE_PORT"
echo "$COORD" > "$STATE"

sleep 3
DIR=$(pwd)
CLIENT_NODE=${NODES[$((1 % NUM_NODES))]}
DASH1_NODE=${NODES[$((2 % NUM_NODES))]}
DASH2_NODE=${NODES[$((3 % NUM_NODES))]}
echo "================================================================"
echo "coordinator : $COORD   (strategy $STRATEGY)"
echo "workers     : ${ADDRESSES[*]}"
echo "logs        : $DIR/logs/"
echo "================================================================"
echo ""
echo "Open a new terminal for each of these (log in to rce first):"
echo ""
echo "  # dashboard 1"
echo "  ssh $DASH1_NODE"
echo "  cd $DIR && .venv/bin/python dashboard.py $COORD"
echo ""
echo "  # dashboard 2"
echo "  ssh $DASH2_NODE"
echo "  cd $DIR && .venv/bin/python dashboard.py $COORD"
echo ""
echo "  # streaming client (100,000 records/s so the dashboards can follow it)"
echo "  ssh $CLIENT_NODE"
echo "  cd $DIR && .venv/bin/python ../q8/generate_dataset.py --n 1000000 --k 10 --s 100 --seed 42 --out data_1M.txt"
echo "  cd $DIR && .venv/bin/python client.py $COORD data_1M.txt 1000 100000"
echo ""
echo "  # final result in the HW2 format"
echo "  cd $DIR && .venv/bin/python dashboard.py $COORD --once"
echo ""
echo "When done: ./run_cluster.sh stop   and then   exit"
