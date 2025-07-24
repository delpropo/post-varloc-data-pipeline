#!/bin/bash

#sbatch       --account=margit0
#sbatch       --partition=standard
#sbatch       -c 1
#sbatch       --nodes=1
#sbatch       --mem-per-cpu=6g
#sbatch       --time=01-00:10:00

# creates a slurm job where you have access with the specified resources.  The job is interactive and you can run commands in the terminal.
srun -c 4 --mem-per-cpu=7g --time=00-10:10:00 --nodes=1 --pty bash -i


