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
filtered_files=$(grep -E '\.(bcf|vcf)$' "$bcf_files_list")

# Check if any files do not end with .bcf or .vcf
invalid_files=$(grep -Ev '\.(bcf|vcf)$' "$bcf_files_list" | grep -v '^\s*$')
if [ -n "$invalid_files" ]; then
    echo "Error: The following files do not end with .bcf or .vcf:"
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

# create an output file in the config directory which will contain the list of files that were processed.  Include the date and time of processing and include vembrane_processing in the file name
output_file="processed_files_$(date +%Y%m%d_%H%M%S)_vembrane_processing.txt"


# Iterate through each file and run vembrane
while IFS= read -r file; do
    dir=$(dirname "$file")
    mkdir -p "$dir/post-varloc-data-pipeline"
    mkdir -p "$dir/post-varloc-data-pipeline/tsv"
    # Skip blank lines
    [ -z "$file" ] && continue
    out_tsv="$dir/post-varloc-data-pipeline/tsv/$(basename "$file").tsv"
    sbatch --account="$slurm_account" --time=24:00:00 --cpus-per-task=1 --mem=7G --job-name=vembrane_table \
        --output="$out_tsv.slurm.out" --wrap="source /home/\$USER/.bashrc && conda activate post-varloc-data-pipeline && vembrane table ALL '$file' > '$out_tsv' 2> /dev/null"
    # Add the complete path of the processed file to the output_file
    echo "$out_tsv" >> "$output_file"
done <<< "$filtered_files"



