#!/bin/bash

#sbatch       --account=margit0
#sbatch       --partition=standard
#sbatch       -c 1
#sbatch       --nodes=1
#sbatch       --mem-per-cpu=4g
#sbatch       --time=00-00:10:00

# creates a slurm job where you have access with the specified resources.  The job is interactive and you can run commands in the terminal.
srun --pty bash -i


