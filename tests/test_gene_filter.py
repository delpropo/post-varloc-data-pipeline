#!/usr/bin/env python3
"""
Test script to verify gene filtering functionality in zarr_pivot_creator.py
"""
import pandas as pd
import sys

# Add the post_varloc_data_pipeline directory to Python path
sys.path.insert(0, '/home/delpropo/github/post-varloc-data-pipeline')

from post_varloc_data_pipeline.zarr_pivot_creator import ZarrFilterPivotCreator

def test_gene_filter():
    """Test the gene filtering functionality."""
    print("Testing gene filtering functionality...")

    # Create a test dataframe that simulates zarr data
    test_data = {
        'CHROM': ['chr1', 'chr2', 'chr3', 'chr4', 'chr5'],
        'POS': [100, 200, 300, 400, 500],
        'REF': ['A', 'C', 'G', 'T', 'A'],
        'ALT': ['G', 'T', 'A', 'C', 'T'],
        "ANN['SYMBOL']": ['A2M', 'BRCA1;TP53', 'UNKNOWN', 'A2M;OTHER', 'NOTFOUND'],
        'SAMPLE': ['S1', 'S2', 'S3', 'S4', 'S5']
    }

    df = pd.DataFrame(test_data)
    print(f"Original test data ({len(df)} rows):")
    print(df)
    print()

    # Create ZarrFilterPivotCreator instance
    creator = ZarrFilterPivotCreator()

    # Load the test gene filter
    gene_filter_path = '/home/delpropo/github/post-varloc-data-pipeline/tests/test_gene_filter.tsv'
    creator.load_gene_filter(gene_filter_path)

    # Apply gene filter
    filtered_df = creator.apply_gene_filter(df)

    print(f"Filtered data ({len(filtered_df)} rows):")
    print(filtered_df)
    print()

    # Check results
    expected_rows = 3  # A2M, BRCA1;TP53, A2M;OTHER should match
    if len(filtered_df) == expected_rows:
        print("✓ Gene filtering test PASSED!")
        print(f"  Expected {expected_rows} rows, got {len(filtered_df)} rows")
    else:
        print("✗ Gene filtering test FAILED!")
        print(f"  Expected {expected_rows} rows, got {len(filtered_df)} rows")

    return len(filtered_df) == expected_rows

if __name__ == "__main__":
    test_gene_filter()
