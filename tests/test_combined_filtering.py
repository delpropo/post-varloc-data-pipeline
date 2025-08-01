#!/usr/bin/env python3
"""
Test script to verify that GENE_FILTER and BED_FILE work together with OR logic

This test verifies that when both gene filtering and BED region filtering are enabled,
rows are kept if they match EITHER condition (OR logic), not both (AND logic).
"""

import pandas as pd
import xarray as xr
import sys
import os
from pathlib import Path

# Add the project root to path
sys.path.insert(0, str(Path(__file__).parent))
from post_varloc_data_pipeline.additional_zarr_filtering import AdditionalZarrFilter

def create_combined_test_data():
    """Create test data to verify OR logic between gene and BED filtering."""
    print("Creating combined test data...")

    # Test data designed to test OR logic:
    # - Some rows match GENE filter but NOT BED regions
    # - Some rows match BED regions but NOT GENE filter
    # - Some rows match BOTH
    # - Some rows match NEITHER (should be filtered out)
    test_data = {
        'CHROM': ['1', '1', '1', '1', '2', '2', '3', '3', '4', '4'],
        'POS': [50, 150, 250, 350, 200, 900, 1500, 1800, 100, 200],
        'REF': ['A', 'G', 'C', 'A', 'G', 'T', 'C', 'A', 'T', 'G'],
        'ALT': ['T', 'C', 'T', 'G', 'T', 'A', 'G', 'C', 'A', 'C'],
        'ID': ['rs001', 'rs002', 'rs003', 'rs004', 'rs005', 'rs006', 'rs007', 'rs008', 'rs009', 'rs010'],
        "ANN['SYMBOL']": ['BRCA1', 'OTHER1', 'BRCA2', 'OTHER2', 'TP53', 'OTHER3', 'BRCA1', 'OTHER4', 'TP53', 'OTHER5'],
        'SAMPLE': ['sample1'] * 10
    }

    df = pd.DataFrame(test_data)

    # Expected logic:
    # BED regions: chr1:100-200, chr1:300-400, chr2:150-250, chr3:1000-2000
    # Gene filter: BRCA1, BRCA2, TP53
    #
    # Row analysis:
    # Chr1, Pos 50,  BRCA1  -> Gene YES, BED NO  -> KEEP (OR logic)
    # Chr1, Pos 150, OTHER1 -> Gene NO,  BED YES -> KEEP (OR logic)
    # Chr1, Pos 250, BRCA2  -> Gene YES, BED NO  -> KEEP (OR logic)
    # Chr1, Pos 350, OTHER2 -> Gene NO,  BED YES -> KEEP (OR logic)
    # Chr2, Pos 200, TP53   -> Gene YES, BED YES -> KEEP (both)
    # Chr2, Pos 900, OTHER3 -> Gene NO,  BED NO  -> FILTER OUT
    # Chr3, Pos 1500, BRCA1 -> Gene YES, BED YES -> KEEP (both)
    # Chr3, Pos 1800, OTHER4-> Gene NO,  BED YES -> KEEP (OR logic)
    # Chr4, Pos 100, TP53   -> Gene YES, BED NO  -> KEEP (OR logic)
    # Chr4, Pos 200, OTHER5 -> Gene NO,  BED NO  -> FILTER OUT

    expected_kept = [
        ('1', 50, 'BRCA1'),   # Gene match only
        ('1', 150, 'OTHER1'), # BED match only
        ('1', 250, 'BRCA2'),  # Gene match only
        ('1', 350, 'OTHER2'), # BED match only
        ('2', 200, 'TP53'),   # Both match
        ('3', 1500, 'BRCA1'), # Both match
        ('3', 1800, 'OTHER4'),# BED match only
        ('4', 100, 'TP53'),   # Gene match only
    ]

    expected_filtered_out = [
        ('2', 900, 'OTHER3'),  # Neither match
        ('4', 200, 'OTHER5'),  # Neither match
    ]

    print(f"Test data created with {len(df)} rows")
    print("\nExpected to be KEPT (gene OR BED match):")
    for chrom, pos, gene in expected_kept:
        print(f"  Chr {chrom}, Pos {pos}, Gene {gene}")

    print("\nExpected to be FILTERED OUT (neither match):")
    for chrom, pos, gene in expected_filtered_out:
        print(f"  Chr {chrom}, Pos {pos}, Gene {gene}")

    return df, expected_kept, expected_filtered_out

def create_test_gene_filter():
    """Create test gene filter file."""
    print("Creating test gene filter file...")

    gene_data = {
        'Symbol': ['BRCA1', 'BRCA2', 'TP53']
    }

    gene_df = pd.DataFrame(gene_data)
    gene_file = Path('test_genes.xlsx')
    gene_df.to_excel(gene_file, index=False)

    print(f"Gene filter file created: {gene_file}")
    print("Gene filter symbols:", list(gene_data['Symbol']))

    return gene_file

def create_test_bed_file():
    """Create test BED file."""
    print("Creating test BED file...")

    bed_data = [
        ['chr1', 100, 200],
        ['chr1', 300, 400],
        ['chr2', 150, 250],
        ['chr3', 1000, 2000]
    ]

    bed_file = Path('test_regions.bed')
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

def test_combined_filtering():
    """Test that gene and BED filtering work together with OR logic."""
    print("\n" + "="*70)
    print("TESTING COMBINED GENE AND BED FILTERING (OR LOGIC)")
    print("="*70)

    # Create test files
    test_df, expected_kept, expected_filtered_out = create_combined_test_data()
    gene_file = create_test_gene_filter()
    bed_file = create_test_bed_file()

    # Convert test data to Zarr
    zarr_input = Path('test_combined_input.zarr')
    df_to_zarr(test_df, zarr_input)

    # Create test config with BOTH filters enabled
    config_content = f"""ADDITIONAL_ZARR_FILTERING:
  OUTPUT_DIR: test_combined_output
  GENE_FILTER: {gene_file}
  BED_FILE: {bed_file}
  DROP_COLUMNS: []"""

    config_file = Path('test_combined_config.yaml')
    with open(config_file, 'w') as f:
        f.write(config_content)

    print(f"\nTest config created: {config_file}")
    print("Config includes BOTH gene filter and BED file")

    # Test the combined filtering
    print("\n" + "-"*50)
    print("RUNNING COMBINED FILTERING TEST")
    print("-"*50)

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
        print("\n" + "-"*50)
        print("ANALYZING RESULTS")
        print("-"*50)

        print(f"Original data: {len(test_df)} rows")
        print(f"Filtered data: {len(filtered_df)} rows")

        # Check which rows were kept
        kept_rows = []
        for _, row in filtered_df.iterrows():
            chrom = str(row['CHROM'])
            pos = int(row['POS'])
            gene = str(row["ANN['SYMBOL']"])
            kept_rows.append((chrom, pos, gene))

        print(f"\nActual KEPT rows ({len(kept_rows)}):")
        for chrom, pos, gene in kept_rows:
            print(f"  Chr {chrom}, Pos {pos}, Gene {gene}")

        # Check which rows were filtered out
        filtered_out_rows = []
        for _, row in test_df.iterrows():
            chrom = str(row['CHROM'])
            pos = int(row['POS'])
            gene = str(row["ANN['SYMBOL']"])
            if (chrom, pos, gene) not in kept_rows:
                filtered_out_rows.append((chrom, pos, gene))

        print(f"\nActual FILTERED OUT rows ({len(filtered_out_rows)}):")
        for chrom, pos, gene in filtered_out_rows:
            print(f"  Chr {chrom}, Pos {pos}, Gene {gene}")

        # Compare with expected results
        print("\n" + "-"*50)
        print("VALIDATION - OR LOGIC CHECK")
        print("-"*50)

        expected_kept_set = set(expected_kept)
        actual_kept_set = set(kept_rows)
        expected_filtered_set = set(expected_filtered_out)
        actual_filtered_set = set(filtered_out_rows)

        # Check kept rows
        correct_kept = expected_kept_set == actual_kept_set
        if correct_kept:
            print("✓ KEPT rows match expected results (OR logic working)")
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
            print("✓ FILTERED OUT rows match expected results (OR logic working)")
        else:
            print("✗ FILTERED OUT rows do NOT match expected results")
            missing_filtered = expected_filtered_set - actual_filtered_set
            extra_filtered = actual_filtered_set - expected_filtered_set
            if missing_filtered:
                print(f"  Missing (should be filtered): {missing_filtered}")
            if extra_filtered:
                print(f"  Extra (shouldn't be filtered): {extra_filtered}")

        # Test OR logic specifically
        print("\n" + "-"*50)
        print("OR LOGIC VERIFICATION")
        print("-"*50)

        # Count matches for each filter type
        gene_only = 0
        bed_only = 0
        both_match = 0
        neither_match = 0

        for _, row in filtered_df.iterrows():
            chrom = str(row['CHROM']).lower().replace('chr', '')
            pos = int(row['POS'])
            gene = str(row["ANN['SYMBOL']"])

            # Check gene match
            gene_match = gene in ['BRCA1', 'BRCA2', 'TP53']

            # Check BED match
            bed_match = False
            bed_regions = [('1', 100, 200), ('1', 300, 400), ('2', 150, 250), ('3', 1000, 2000)]
            for bed_chrom, bed_start, bed_end in bed_regions:
                if chrom == str(bed_chrom) and bed_start <= pos <= bed_end:
                    bed_match = True
                    break

            if gene_match and bed_match:
                both_match += 1
            elif gene_match and not bed_match:
                gene_only += 1
            elif not gene_match and bed_match:
                bed_only += 1
            else:
                neither_match += 1

        print(f"Gene filter only matches: {gene_only}")
        print(f"BED region only matches: {bed_only}")
        print(f"Both filters match: {both_match}")
        print(f"Neither filter matches: {neither_match}")

        # OR logic should keep gene_only + bed_only + both_match
        # AND logic would only keep both_match
        or_expected = gene_only + bed_only + both_match
        if len(filtered_df) == or_expected and neither_match == 0:
            print("✓ OR LOGIC CONFIRMED: Keeping rows that match EITHER filter")
        else:
            print("✗ OR LOGIC FAILED: Not working as expected")

        # Overall result
        if correct_kept and correct_filtered and neither_match == 0:
            print("\n🎉 TEST PASSED: Combined filtering with OR logic works correctly!")
            return True
        else:
            print("\n❌ TEST FAILED: Combined filtering has issues!")
            return False

    except Exception as e:
        print(f"\n❌ TEST ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        # Cleanup
        print("\n" + "-"*50)
        print("CLEANUP")
        print("-"*50)

        cleanup_files = [
            zarr_input,
            config_file,
            gene_file,
            bed_file,
            Path('test_combined_output')
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
    success = test_combined_filtering()
    sys.exit(0 if success else 1)
