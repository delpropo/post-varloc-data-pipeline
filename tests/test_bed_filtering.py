#!/usr/bin/env python3
"""
Test script for BED filtering functionality in additional_zarr_filtering.py

This script creates test data, converts it to Zarr format, and tests BED filtering.
"""

import pandas as pd
import xarray as xr
import sys
import os
from pathlib import Path

# Add the project root to path
sys.path.insert(0, str(Path(__file__).parent))
from post_varloc_data_pipeline.additional_zarr_filtering import AdditionalZarrFilter

def create_test_data():
    """Create test genomic data with known positions."""
    print("Creating test data...")

    # Test data with specific positions relative to our BED regions
    # BED regions: chr1:100-200, chr1:500-600, chr2:1000-1100, chr3:2000-2500
    test_data = {
        'CHROM': ['1', '1', '1', '1', '2', '2', '3', '3', '4'],
        'POS': [50, 150, 250, 550, 900, 1050, 1500, 2200, 3000],  # positions
        'REF': ['A', 'G', 'C', 'A', 'T', 'G', 'C', 'A', 'T'],
        'ALT': ['T', 'C', 'T', 'G', 'A', 'T', 'G', 'C', 'G'],
        'ID': ['rs001', 'rs002', 'rs003', 'rs004', 'rs005', 'rs006', 'rs007', 'rs008', 'rs009'],
        "ANN['SYMBOL']": ['GENE1', 'GENE2', 'GENE3', 'GENE4', 'GENE5', 'GENE6', 'GENE7', 'GENE8', 'GENE9']
    }

    df = pd.DataFrame(test_data)

    # Expected results based on BED regions:
    # Chr1: 50 (OUT), 150 (IN: 100-200), 250 (OUT), 550 (IN: 500-600)
    # Chr2: 900 (OUT), 1050 (IN: 1000-1100)
    # Chr3: 1500 (OUT), 2200 (IN: 2000-2500)
    # Chr4: 3000 (OUT - no BED region for chr4)

    expected_kept = [
        ('1', 150),  # GENE2 - in region 100-200
        ('1', 550),  # GENE4 - in region 500-600
        ('2', 1050), # GENE6 - in region 1000-1100
        ('3', 2200)  # GENE8 - in region 2000-2500
    ]

    expected_filtered_out = [
        ('1', 50),   # GENE1 - before region 100-200
        ('1', 250),  # GENE3 - after region 100-200, before 500-600
        ('2', 900),  # GENE5 - before region 1000-1100
        ('3', 1500), # GENE7 - before region 2000-2500
        ('4', 3000)  # GENE9 - no BED region for chr4
    ]

    print(f"Test data created with {len(df)} rows")
    print("Expected to be KEPT (inside BED regions):")
    for chrom, pos in expected_kept:
        print(f"  Chr {chrom}, Pos {pos}")

    print("Expected to be FILTERED OUT (outside BED regions):")
    for chrom, pos in expected_filtered_out:
        print(f"  Chr {chrom}, Pos {pos}")

    return df, expected_kept, expected_filtered_out

def create_bed_file():
    """Create test BED file."""
    print("Creating test BED file...")

    bed_data = [
        ['chr1', 100, 200],
        ['chr1', 500, 600],
        ['chr2', 1000, 1100],
        ['chr3', 2000, 2500]
    ]

    bed_file = Path('tests/data/additional_filtering/test.bed')
    bed_file.parent.mkdir(parents=True, exist_ok=True)

    with open(bed_file, 'w') as f:
        for chrom, start, end in bed_data:
            f.write(f"{chrom}\t{start}\t{end}\n")

    print(f"BED file created: {bed_file}")
    print("BED regions:")
    for chrom, start, end in bed_data:
        print(f"  {chrom}: {start}-{end}")

    return bed_file

def df_to_zarr(df, zarr_path):
    """Convert DataFrame to Zarr format."""
    print(f"Converting DataFrame to Zarr: {zarr_path}")

    # Convert to xarray dataset
    ds = df.to_xarray()

    # Save as Zarr
    zarr_path.parent.mkdir(parents=True, exist_ok=True)
    ds.to_zarr(str(zarr_path), mode='w')

    print(f"Zarr file created: {zarr_path}")
    return zarr_path

def test_bed_filtering():
    """Test BED filtering functionality."""
    print("\n" + "="*60)
    print("TESTING BED FILTERING FUNCTIONALITY")
    print("="*60)

    # Create test files
    test_df, expected_kept, expected_filtered_out = create_test_data()
    bed_file = create_bed_file()

    # Convert test data to Zarr
    zarr_input = Path('tests/data/additional_filtering/test_input.zarr')
    df_to_zarr(test_df, zarr_input)

    # Create test config
    config_content = f"""ADDITIONAL_ZARR_FILTERING:
  OUTPUT_DIR: tests/data/additional_filtering/output
  BED_FILE: {bed_file}
  DROP_COLUMNS: []"""

    config_file = Path('test_config.yaml')
    with open(config_file, 'w') as f:
        f.write(config_content)

    print(f"\nTest config created: {config_file}")

    # Test the filtering
    print("\n" + "-"*40)
    print("RUNNING BED FILTERING TEST")
    print("-"*40)

    try:
        # Initialize filter with test config
        filter_processor = AdditionalZarrFilter(str(config_file))

        # Process the test file
        output_path, filtered_df = filter_processor.process_zarr_file(
            input_path=str(zarr_input)
        )

        print(f"\nFiltering completed!")
        print(f"Output saved to: {output_path}")

        # Analyze results
        print("\n" + "-"*40)
        print("ANALYZING RESULTS")
        print("-"*40)

        print(f"Original data: {len(test_df)} rows")
        print(f"Filtered data: {len(filtered_df)} rows")

        # Check which rows were kept
        kept_rows = []
        for _, row in filtered_df.iterrows():
            chrom = str(row['CHROM'])
            pos = int(row['POS'])
            kept_rows.append((chrom, pos))

        print(f"\nActual KEPT rows ({len(kept_rows)}):")
        for chrom, pos in kept_rows:
            print(f"  Chr {chrom}, Pos {pos}")

        # Check which rows were filtered out
        filtered_out_rows = []
        for _, row in test_df.iterrows():
            chrom = str(row['CHROM'])
            pos = int(row['POS'])
            if (chrom, pos) not in kept_rows:
                filtered_out_rows.append((chrom, pos))

        print(f"\nActual FILTERED OUT rows ({len(filtered_out_rows)}):")
        for chrom, pos in filtered_out_rows:
            print(f"  Chr {chrom}, Pos {pos}")

        # Compare with expected results
        print("\n" + "-"*40)
        print("VALIDATION")
        print("-"*40)

        expected_kept_set = set(expected_kept)
        actual_kept_set = set(kept_rows)
        expected_filtered_set = set(expected_filtered_out)
        actual_filtered_set = set(filtered_out_rows)

        # Check kept rows
        correct_kept = expected_kept_set == actual_kept_set
        if correct_kept:
            print("✓ KEPT rows match expected results")
        else:
            print("✗ KEPT rows do NOT match expected results")
            missing_kept = expected_kept_set - actual_kept_set
            extra_kept = actual_kept_set - expected_kept_set
            if missing_kept:
                print(f"  Missing (should be kept): {missing_kept}")
            if extra_kept:
                print(f"  Extra (shouldn't be kept): {extra_kept}")

        # Check filtered out rows
        correct_filtered = expected_filtered_set == actual_filtered_set
        if correct_filtered:
            print("✓ FILTERED OUT rows match expected results")
        else:
            print("✗ FILTERED OUT rows do NOT match expected results")
            missing_filtered = expected_filtered_set - actual_filtered_set
            extra_filtered = actual_filtered_set - expected_filtered_set
            if missing_filtered:
                print(f"  Missing (should be filtered): {missing_filtered}")
            if extra_filtered:
                print(f"  Extra (shouldn't be filtered): {extra_filtered}")

        # Overall result
        if correct_kept and correct_filtered:
            print("\n🎉 TEST PASSED: BED filtering works correctly!")
            return True
        else:
            print("\n❌ TEST FAILED: BED filtering has issues!")
            return False

    except Exception as e:
        print(f"\n❌ TEST ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        # Cleanup
        print("\n" + "-"*40)
        print("CLEANUP")
        print("-"*40)

        cleanup_files = [
            zarr_input,
            config_file,
            bed_file,
            Path('tests/data/additional_filtering/output')
        ]

        for path in cleanup_files:
            if path.exists():
                if path.is_dir():
                    import shutil
                    shutil.rmtree(path)
                    print(f"Removed directory: {path}")
                else:
                    path.unlink()
                    print(f"Removed file: {path}")

if __name__ == "__main__":
    success = test_bed_filtering()
    sys.exit(0 if success else 1)
