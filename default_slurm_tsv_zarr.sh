#!/bin/bash

source /home/$USER/.bashrc

# add the TMPDIR in scratch to use
export TMPDIR=/scratch/

conda activate post-varloc-data-pipeline
# conda activate sgkit_env

# The run_processing script will be added to the end when run
sbatch --account= \
    --partition=largemem \
    --mem-per-cpu=30g  \
    --cpus-per-task=1 \
    --time=4:00:00 \

##    run_processing.sh
