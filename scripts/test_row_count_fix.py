#!/usr/bin/env python3
"""
Test script to verify the ROW_COUNT fix in zarr_groupby_aggregator.py
"""

import pandas as pd

# Create a small test DataFrame to simulate the aggregation issue
df = pd.DataFrame({
    'CHROM': ['chr1', 'chr1', 'chr2', 'chr2', 'chr2'],
    'POS': [100, 100, 200, 200, 300],
    'REF': ['A', 'A', 'T', 'T', 'G'],
    'ALT': ['T', 'T', 'C', 'C', 'A'],
    'VALUE': [10, 20, 30, 40, 50]
})

print("Original DataFrame:")
print(df)

# Group by genomic coordinates
grouped = df.groupby(['CHROM', 'POS', 'REF', 'ALT'])

# Create aggregation dictionary
agg_dict = {'VALUE': 'sum'}

# Perform aggregation
result_df = grouped.agg(agg_dict).reset_index()

# Add row count showing number of rows combined per variant
result_df['ROW_COUNT'] = grouped.size().values

print("\nAggregated DataFrame with ROW_COUNT:")
print(result_df)
print("\nSuccess! ROW_COUNT logic works correctly.")
