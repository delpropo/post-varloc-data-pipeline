#!/usr/bin/env python3
"""
Extract ANN data types from ann_types.md and create a configuration file
for use with tsv_preprocess_to_zarr.py
"""

import re
import csv
import argparse
from pathlib import Path
from typing import Dict, List, Tuple


def parse_markdown_table(content: str, section_name: str) -> List[Tuple[str, str]]:
    """
    Parse a markdown table and extract column name and type information.

    Args:
        content (str): Markdown content
        section_name (str): Section name (e.g., "vep", "snpEff")

    Returns:
        List[Tuple[str, str]]: List of (column_name, data_type) tuples
    """
    results = []

    # Find the section
    section_pattern = rf"## {section_name}.*?(?=##|\Z)"
    section_match = re.search(section_pattern, content, re.DOTALL | re.IGNORECASE)

    if not section_match:
        print(f"Warning: Section '{section_name}' not found")
        return results

    section_content = section_match.group(0)

    # Find tables with custom types
    custom_types_pattern = r"Annotations with custom types:.*?\n\n(.*?)(?=\n\n|\Z)"
    custom_match = re.search(custom_types_pattern, section_content, re.DOTALL)

    if custom_match:
        table_content = custom_match.group(1)
        # Parse table rows (skip header and separator)
        lines = table_content.strip().split('\n')
        for line in lines[2:]:  # Skip header and separator
            if line.strip() and line.startswith('|'):
                parts = [part.strip() for part in line.split('|')[1:-1]]  # Remove empty first/last
                if len(parts) >= 2:
                    column_name = parts[0].strip('`')
                    type_info = parts[1]
                    results.append((column_name, type_info))

    return results


def map_vep_types_to_pandas(type_info: str) -> str:
    """
    Map VEP type information to pandas/numpy dtypes.

    Args:
        type_info (str): Type information from markdown

    Returns:
        str: Corresponding pandas dtype
    """
    type_info = type_info.lower()

    # Direct mappings
    if 'int' in type_info:
        return 'int64'
    elif 'float' in type_info:
        return 'float64'
    elif 'bool' in type_info:
        return 'boolean'
    elif 'list[str]' in type_info or 'list[term]' in type_info:
        return 'string'  # Will be stored as string representation
    elif 'dict[str, float]' in type_info or 'dict[str, any]' in type_info:
        return 'string'  # Will be stored as string representation
    elif 'consequences' in type_info or 'list[term]' in type_info:
        return 'string'
    elif 'posrange' in type_info or 'rangetotal' in type_info:
        return 'string'  # Complex types stored as strings
    else:
        return 'string'  # Default fallback


def create_dtype_config(ann_types: List[Tuple[str, str]], output_file: str, prefix: str = "ANN['", suffix: str = "']") -> None:
    """
    Create a CSV configuration file with column names and data types.

    Args:
        ann_types (List[Tuple[str, str]]): List of (column_name, type_info) tuples
        output_file (str): Output CSV file path
        prefix (str): Prefix to add to column names (e.g., "ANN['")
        suffix (str): Suffix to add to column names (e.g., "']")
    """
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['column_name', 'dtype'])

        for column_name, type_info in ann_types:
            full_column_name = f"{prefix}{column_name}{suffix}"
            pandas_dtype = map_vep_types_to_pandas(type_info)
            writer.writerow([full_column_name, pandas_dtype])


def main():
    """Main function to extract ANN types and create configuration files."""
    parser = argparse.ArgumentParser(description='Extract ANN types from markdown and create dtype config')
    parser.add_argument('--input', '-i', default='references/ann_types.md',
                       help='Input markdown file (default: references/ann_types.md)')
    parser.add_argument('--output-dir', '-o', default='config',
                       help='Output directory for config files (default: config)')
    parser.add_argument('--prefix', default="ANN['",
                       help='Prefix for column names (default: "ANN[\'")')
    parser.add_argument('--suffix', default="']",
                       help='Suffix for column names (default: "\']")')

    args = parser.parse_args()

    # Read the markdown file
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Error: Input file {input_path} does not exist")
        return

    with open(input_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True)

    # Extract VEP types
    vep_types = parse_markdown_table(content, "vep")
    if vep_types:
        vep_output = output_dir / "vep_ann_dtypes.csv"
        create_dtype_config(vep_types, str(vep_output), args.prefix, args.suffix)
        print(f"Created VEP dtype config: {vep_output}")
        print(f"Found {len(vep_types)} VEP ANN columns with custom types")

    # Extract snpEff types
    snpeff_types = parse_markdown_table(content, "snpEff")
    if snpeff_types:
        snpeff_output = output_dir / "snpeff_ann_dtypes.csv"
        create_dtype_config(snpeff_types, str(snpeff_output), args.prefix, args.suffix)
        print(f"Created snpEff dtype config: {snpeff_output}")
        print(f"Found {len(snpeff_types)} snpEff ANN columns with custom types")

    # Create combined config in references directory
    all_types = vep_types + snpeff_types
    if all_types:
        references_dir = Path("references")
        references_dir.mkdir(exist_ok=True)
        combined_output = references_dir / "combined_ann_dtypes.csv"
        create_dtype_config(all_types, str(combined_output), args.prefix, args.suffix)
        print(f"Created combined dtype config: {combined_output}")
        print(f"Total unique ANN columns: {len(set(col for col, _ in all_types))}")


if __name__ == "__main__":
    main()
