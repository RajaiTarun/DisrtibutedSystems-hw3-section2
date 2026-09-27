#!/bin/bash
#SBATCH --job-name=q8-test
#SBATCH --ntasks=9
#SBATCH --nodes=1
#SBATCH --mem-per-cpu=4G
#SBATCH --time=02:00:00
#SBATCH --output=%j.log
#SBATCH --error=%j.err
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=tarun.rajai@students.iiit.ac.in
#SBATCH -D /home/cs3401.49/ds-hw2/q8

# Load necessary modules
module load openmpi/4.1.5

echo "========================================="
echo "SLURM Job ID: $SLURM_JOB_ID"
echo "Allocated nodes: $SLURM_NNODES"
echo "Total tasks: $SLURM_NTASKS"
echo "Node list: $SLURM_NODELIST"
echo "========================================="
echo ""

# Compile sequential program
echo "Compiling sequential program..."
g++ -O2 -std=c++17 sequential.cpp q8_common.cpp -o sequential
if [ $? -ne 0 ]; then
    echo "Sequential compilation failed!"
    exit 1
fi

# Compile MPI program
echo "Compiling MPI program..."
mpicxx -O2 -std=c++17 mpi.cpp q8_common.cpp -o mpi_q8
if [ $? -ne 0 ]; then
    echo "MPI compilation failed!"
    exit 1
fi
echo "Compilation successful."
echo ""

# small input generated inline (not read from testcases/) so this job
# doesn't depend on what did or didn't get synced to the cluster - this is
# a smoke test (does everything build and run correctly in this cluster's
# module/network environment), not the real benchmark - that's a separate
# script with larger datasets and multiple process counts.
IN=in.txt
cat > "$IN" << 'EOF'
10 3 4
10  0 20.0 50.0 1000.0 1.0 4.0
25  1 30.0 60.0 1010.0 2.0 6.0
50  0 40.0 70.0 1020.0 3.0 8.0
70  2 10.0 40.0 990.0  0.0 2.0
90  1 35.0 80.0 1030.0 4.0 10.0
110 0 25.0 55.0 1005.0 2.0 5.0
130 2 0.0  45.0 995.0  5.0 3.0
150 3 45.0 65.0 1015.0 6.0 7.0
170 1 15.0 35.0 985.0  1.0 1.0
190 0 30.0 75.0 1025.0 3.0 9.0
EOF

echo "=== Running sequential ==="
{ time ./sequential < "$IN" > seq_output.txt; } > seq_time.txt 2>&1
cat seq_time.txt
echo ""

# same UCX workaround as q2/q6: UCX isn't installed on this cluster, so
# OpenMPI's default transport selection prints noisy warnings/fails to
# init. --mca pml ob1 forces the standard point-to-point protocol instead,
# and --mca osc ^ucx excludes UCX from one-sided communication selection.
# vader's default CMA single-copy path fails with "Read -1, errno=1" on this
# cluster (ptrace_scope restrictions), so force the non-CMA (double-copy)
# vader path instead of falling back to slow TCP loopback.
echo "=== Running MPI (np=9) ==="
{ time mpirun -np 9 --mca pml ob1 --mca osc ^ucx --mca btl vader,self --mca btl_vader_single_copy_mechanism none ./mpi_q8 < "$IN" > mpi_output.txt; } > mpi_time.txt 2>&1
cat mpi_time.txt
echo ""

echo "=== Correctness check: sequential vs MPI output ==="
if diff -q seq_output.txt mpi_output.txt > /dev/null; then
    echo "PASS: MPI output matches sequential output"
else
    echo "FAIL: MPI output does not match sequential output"
    diff seq_output.txt mpi_output.txt
fi
