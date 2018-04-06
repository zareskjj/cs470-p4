#!/bin/bash
#
#SBATCH --job-name=non-determ

salloc -n 16 mpirun ./dht consistency.txt
echo "Waiting on output..."
sleep 5
cat dump* consistency.txt | less
