#!/bin/bash



source /home/$USER/.bashrc
wait

# this implements proxy settings for the slurm job
source /etc/profile.d/http_proxy.sh

conda activate post-varloc-data-pipeline
wait

python test.py
