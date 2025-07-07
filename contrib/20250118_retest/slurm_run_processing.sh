#!/bin/bash

# export PATH="/home/delpropo/miniconda3/etc/profile.d/conda.sh"
source /home/delpropo/.bashrc

export TMPDIR=/scratch/sooeunc_root/sooeunc0/delpropo

conda activate sgkit_env

# --mem=40G
sbatch --account=sooeunc0 \
    --partition=largemem \
    --mem-per-cpu=70g  \
    --cpus-per-task=1 \
    --time=5:00:00 \
    run_processing.sh
