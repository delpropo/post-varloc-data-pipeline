#!/bin/bash
source /home/$USER/.bashrc
conda activate post-varloc-data-pipeline

if ! command -v vembrane &> /dev/null; then
    echo "Error: vembrane command not found. Please install vembrane or activate your conda environment."
    exit 1
fi

# Check if a file argument is provided
if [ $# -ne 1 ]; then
    echo "Usage: $0 <bcf_files_list.txt>"
    exit 1
fi

bcf_files_list="$1"
echo "File being used: $bcf_files_list:"

if [ ! -f "$bcf_files_list" ]; then
    echo "File not found: $bcf_files_list"
    exit 1
fi

# Filter the list to keep only lines ending with .bcf or .vcf
filtered_files=$(grep -E '\.(tsv)$' "$bcf_files_list")

# Check if any files do not end with .tsv
invalid_files=$(grep -Ev '\.(tsv)$' "$bcf_files_list" | grep -v '^\s*$')
if [ -n "$invalid_files" ]; then
    echo "Error: The following files do not end with .tsv:"
    echo "$invalid_files"
    exit 1
fi

# Confirm that all files in filtered_files exist
missing_files=()
while IFS= read -r file; do
    [ -z "$file" ] && continue
    if [ ! -f "$file" ]; then
        missing_files+=("$file")
    fi
done <<< "$filtered_files"

if [ ${#missing_files[@]} -ne 0 ]; then
    echo "Error: The following files do not exist:"
    for f in "${missing_files[@]}"; do
        echo "$f"
    done
    exit 1
fi

# Get SLURM account from config.ini
config_file="config.ini"
if [ ! -f "$config_file" ]; then
    echo "Error: config.ini not found in current directory."
    exit 1
fi

slurm_account=$(awk -F '=' '/^\s*slurm_account\s*=/ {gsub(/^[ \t]+|[ \t]+$/, "", $2); print $2}' "$config_file")
if [ -z "$slurm_account" ]; then
    echo "Error: slurm_account not set in config.ini."
    exit 1
fi

# Iterate through each file and run vembrane_tsv_to_zarr.py.
while IFS= read -r file; do
    dir=$(dirname "$file")
    mkdir -p "$dir/zarr"
    filesize_bytes=$(stat -c%s "$file")
    filesize_gb=$(awk "BEGIN {printf \"%.2f\", $filesize_bytes/1024/1024/1024}")
    echo "Processing $file (size: $filesize_gb GB)"
    if (( $(echo "$filesize_gb > 5" | bc -l) )); then
        echo "Warning: $file is larger than 5GB. The sbatch job may fail due to insufficient memory."
    fi
    # Skip blank lines
    [ -z "$file" ] && continue
    sbatch --account="$slurm_account" --time=02:00:00 --cpus-per-task=1 --mem=14G --job-name=vembrane_tsv_to_zarr \
        --output="$dir/zarr/$(basename "$file").zarr.slurm.out" \
        --wrap="source /home/\$USER/.bashrc && conda activate post-varloc-data-pipeline && python post_varloc_data_pipeline/vembrane_tsv_to_zarr.py --input '$file' --output '$dir/zarr'"
done <<< "$filtered_files"




