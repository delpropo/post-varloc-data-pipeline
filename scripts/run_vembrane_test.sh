#!/bin/bash
set -e

# Change to project directory
cd /home/delpropo/github/post-varloc-data-pipeline

# Run vembrane and save output
vembrane table ALL tests/data/test_ann_all/test.vcf > tests/data/test_ann_all/output.tsv

# Verify file was created
if [ -f "tests/data/test_ann_all/output.tsv" ]; then
    echo "Output file created successfully"
    ls -la tests/data/test_ann_all/output.tsv
else
    echo "ERROR: Output file was not created"
    exit 1
fi
