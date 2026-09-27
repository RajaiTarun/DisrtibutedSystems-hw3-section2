#!/bin/bash
#SBATCH --job-name=q8-seq-bench
#SBATCH --ntasks=1
#SBATCH --nodes=1
#SBATCH --mem-per-cpu=4G
#SBATCH --time=04:00:00
#SBATCH --output=%j.log
#SBATCH --error=%j.err
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=tarun.rajai@students.iiit.ac.in
#SBATCH -D /home/cs3401.49/ds-hw2/q8

# Cluster sequential benchmark - same node type/partition as bench_mpi.sh,
# same K/S/SEED/SIZES, so output/sequential_benchmark.log is directly
# comparable to output/mpi_benchmark.log (speedup/efficiency numbers are
# only meaningful when both sides ran on identical hardware).
#
# usage: sbatch bench_sequential_cluster.sh

echo "========================================="
echo "SLURM Job ID: $SLURM_JOB_ID"
echo "Allocated nodes: $SLURM_NNODES"
echo "Total tasks: $SLURM_NTASKS"
echo "Node list: $SLURM_NODELIST"
echo "========================================="
echo ""

echo "Compiling sequential program..."
g++ -O2 -std=c++17 sequential.cpp q8_common.cpp -o sequential
if [ $? -ne 0 ]; then
    echo "Sequential compilation failed!"
    exit 1
fi
echo "Compilation successful."
echo ""

mkdir -p output
RESULTS=output/sequential_benchmark.log
printf "" > "$RESULTS"

# must stay identical to bench_mpi.sh's K/S/SEED/SIZES.
K=10
S=100
SEED=42

SIZES=(100 1000 10000 100000 1000000 20000000 50000000)

IN=bench_input_seq.txt

for N in "${SIZES[@]}"; do
    echo "=== N=$N: generating input ==="
    python3 generate_dataset.py --n "$N" --k "$K" --s "$S" --seed "$SEED" --out "$IN"

    echo "N=$N K=$K S=$S seed=$SEED" >> "$RESULTS"
    { time ./sequential < "$IN" > /dev/null; } >> "$RESULTS" 2>&1
    echo "" >> "$RESULTS"

    echo "=== N=$N: deleting input ==="
    rm -f "$IN"
    echo ""
done

echo "========================================="
echo "Benchmark complete. Results in $RESULTS"
echo "========================================="
