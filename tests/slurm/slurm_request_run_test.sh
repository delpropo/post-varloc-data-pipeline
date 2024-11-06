#!/bin/bash


#source ~/.bashrc
# alternative is source /home/$USER/.bashrc



# set your tempfile directory for the run.  Needs to have enough memory.  Default scratch
#export TMPDIR=//scratch/margit_root/margit0


# Set the slurm parameters
#sbatch       --account=margit0
#sbatch       --partition=standard
#sbatch       -c 1
#sbatch       --nodes=1
#sbatch       --mem-per-cpu=7g
#sbatch       --time=00-00:02:00




# source /home/$USER/.bashrc
# source /etc/profile.d/http_proxy.sh

# changes to the directory where the execution script is located
cd "$(dirname "$0")"
srun bash run_working_openAI_test.sh
