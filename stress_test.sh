#!/bin/bash
#
#SBATCH --job-name=stress_test
#SBATCH --output=stress_results.txt
make
srun mpirun -n 4 ./dht stress.txt
echo "Waiting on 4 output..."
sleep 5
echo "Lines of output: "
cat dump* | wc -l
echo "Lines of input: "
cat stress.txt | wc -l

srun mpirun -n 8 ./dht stress.txt
echo "Waiting on 8 output..."
sleep 5
echo "Lines of output: "
cat dump* | wc -l
echo "Lines of input: "
cat stress.txt | wc -l

srun mpirun -n 16 ./dht stress.txt
echo "Waiting on 16 output..."
sleep 5
echo "Lines of output: "
cat dump* | wc -l
echo "Lines of input: "
cat stress.txt | wc -l

srun mpirun -n 32 ./dht stress.txt
echo "Waiting on 32 output..."
sleep 5
echo "Lines of output: "
cat dump* | wc -l
echo "Lines of input: "
cat stress.txt | wc -l

srun mpirun -n 64 ./dht stress.txt
echo "Waiting on 64 output..."
sleep 5
echo "Lines of output: "
cat dump* | wc -l
echo "Lines of input: "
cat stress.txt | wc -l
