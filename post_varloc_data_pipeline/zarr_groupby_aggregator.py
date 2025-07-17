#!/usr/bin/env python3
"""
Zarr Cross-File Aggregator - Combine multiple pivoted Zarr files for genomic variant analysis.

WORKFLOW:
Step 1: Individual files are processed by zarr_pivot_creator.py (filtering + pivot)
Step 2: This script combines processed files with cross-file pivot aggregation

AGGREGATION STRATEGY:
- Pivot on: CHROM, POS, REF, ALT (genomic coordinates only)
- Include specific ANN columns: MAX_AF, VARIANT_CLASS, Feature_type, IMPACT, SYMBOL
- Include all INFO columns
- Handle FORMAT['AF'] by family (preserve as 1_AF, 2_AF, etc    # Dask options
    parser.add_argument('--workers', type=int, help='Number of Dask workers (default: auto-detect all cores)')
    parser.add_argument('--chunk-size', default='1GB', help='Chunk size for Dask processing (default: 1GB)')- Add ROW_COUNT showing number of rows combined per variant
- Save final aggregated results

Optimized for SLURM environments with automatic multi-core processing.
"""

import argparse
import sys
import warnings
from pathlib import Path
from typing import List
import pandas as pd
import xarray as xr
from dask.distributed import Client

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore', message='.*vlen-utf8.*')
warnings.filterwarnings('ignore', message='.*StringDType.*')
warnings.filterwarnings('ignore', message='.*Consolidated metadata.*')


class ZarrCrossFileAggregator:
    """
    Cross-file Zarr aggregator for combining pivoted zarr files.
    Assumes individual files have already been processed by zarr_pivot_creator.py
    """

    def __init__(self, n_workers=None, chunk_size="1GB"):
        """
        Initialize the aggregator with Dask configuration.

        Args:
            n_workers: Number of Dask workers (None = auto-detect all cores)
            chunk_size: Memory size per chunk for processing
        """
        self.chunk_size = chunk_size
        self.client = None
        # Columns for cross-file pivot (genomic coordinates only)
        self.pivot_columns = ['CHROM', 'POS', 'REF', 'ALT']
        # Specific columns to include in aggregation
        self.target_ann_columns = ["ANN['MAX_AF']", "ANN['VARIANT_CLASS']", "ANN['Feature_type']", "ANN['IMPACT']", "ANN['SYMBOL']"]
        self.setup_dask_client(n_workers)

    def setup_dask_client(self, n_workers=None):
        """Setup Dask client for parallel processing."""
        try:
            if n_workers:
                self.client = Client(n_workers=n_workers, threads_per_worker=2, silence_logs=False)
            else:
                # Auto-detect and use all available cores
                self.client = Client(silence_logs=False)

            print("✓ Dask client initialized")
            print(f"  Workers: {len(self.client.scheduler_info()['workers'])}")
            print(f"  Total cores: {sum(w['nthreads'] for w in self.client.scheduler_info()['workers'].values())}")
            print(f"  Dashboard: {self.client.dashboard_link}")

        except Exception as e:
            print(f"Warning: Could not setup Dask client: {e}")
            print("Falling back to single-threaded processing")
            self.client = None

    def load_zarr_files_list(self, file_list_path: str) -> List[str]:
        """
        Load list of Zarr file paths from a text file.

        Args:
            file_list_path: Path to text file containing Zarr file paths (one per line)

        Returns:
            List of validated Zarr file paths
        """
        zarr_files = []

        try:
            with open(file_list_path, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    zarr_path = line.strip()
                    if zarr_path and not zarr_path.startswith('#'):
                        if Path(zarr_path).exists():
                            zarr_files.append(zarr_path)
                        else:
                            print(f"Warning: Zarr file not found (line {line_num}): {zarr_path}")

        except FileNotFoundError:
            print(f"Error: File list not found: {file_list_path}")
            sys.exit(1)

        return zarr_files

    def validate_processed_zarr_files(self, zarr_files: List[str]) -> List[str]:
        """
        Validate that all processed Zarr files exist and are accessible.
        Expects files that have already been processed by zarr_pivot_creator.py

        Args:
            zarr_files: List of processed Zarr file paths

        Returns:
            List of validated processed Zarr file paths
        """
        valid_files = []

        for zarr_file in zarr_files:
            zarr_path = Path(zarr_file)
            if zarr_path.exists() and zarr_path.is_dir():
                try:
                    # Quick check that we can open the zarr file
                    ds = xr.open_zarr(zarr_file)

                    # Check if it looks like a processed file (has FILENAME column)
                    available_columns = list(ds.data_vars.keys()) + list(ds.coords.keys())
                    if 'FILENAME' in available_columns:
                        print(f"✓ Validated processed file: {zarr_file}")
                        valid_files.append(zarr_file)
                    else:
                        print(f"⚠ Warning: {zarr_file} doesn't appear to be processed (missing FILENAME column)")
                        valid_files.append(zarr_file)  # Include anyway

                    ds.close()
                except Exception as e:
                    print(f"✗ Cannot read Zarr file {zarr_file}: {e}")
            else:
                print(f"✗ Zarr file not found: {zarr_file}")

        if not valid_files:
            print("Error: No valid processed Zarr files found!")
            sys.exit(1)

        return valid_files

    def combine_processed_zarr_files(self, processed_files: List[str], output_path: str, export_tsv: bool = False):
        """
        Step 2: Combine processed zarr files with cross-file pivot.

        Args:
            processed_files: List of processed zarr file paths
            output_path: Final output file path
            export_tsv: Whether to also export as TSV
        """
        print(f"Step 2: Combining {len(processed_files)} processed files...")

        try:
            # Load all processed files and validate family uniqueness
            all_dfs = []
            file_families = {}

            for file_idx, zarr_file in enumerate(processed_files, 1):
                ds = xr.open_zarr(zarr_file)
                df = ds.to_dataframe().reset_index()
                ds.close()

                # Check if family column exists and validate uniqueness
                if 'family' in df.columns:
                    unique_families = df['family'].dropna().unique()
                    if len(unique_families) != 1:
                        print(f"Error: File {zarr_file} has multiple or no families: {unique_families}")
                        raise ValueError("Each file must have exactly one unique family identifier")

                    family_name = unique_families[0]
                    if family_name in file_families.values():
                        print(f"Error: Family '{family_name}' appears in multiple files")
                        raise ValueError(f"Family '{family_name}' is not unique across files")

                    file_families[zarr_file] = family_name
                    print(f"  File {file_idx}: Family '{family_name}'")

                    # Rename FORMAT['AF'] column to be family-specific
                    format_af_cols = [col for col in df.columns if col.startswith("FORMAT[") and "'AF'" in col]
                    for af_col in format_af_cols:
                        new_col_name = f"{family_name}_AF"
                        df = df.rename(columns={af_col: new_col_name})
                        print(f"    Renamed {af_col} to {new_col_name}")
                else:
                    print(f"Warning: File {zarr_file} missing 'family' column")
                    # Assign a default family name
                    family_name = f"{file_idx}"
                    df['family'] = family_name
                    file_families[zarr_file] = family_name
                    print(f"  File {file_idx}: Assigned default family '{family_name}'")

                    # Rename FORMAT['AF'] column to be family-specific
                    format_af_cols = [col for col in df.columns if col.startswith("FORMAT[") and "'AF'" in col]
                    for af_col in format_af_cols:
                        new_col_name = f"{family_name}_AF"
                        df = df.rename(columns={af_col: new_col_name})
                        print(f"    Renamed {af_col} to {new_col_name}")

                all_dfs.append(df)

            # Combine all dataframes
            combined_df = pd.concat(all_dfs, ignore_index=True)
            print(f"Combined dataframe shape: {combined_df.shape}")
            print(f"Family mapping: {file_families}")

            # Create properly structured family AF columns
            combined_df = self.create_family_af_columns(combined_df, file_families)

            # Select columns to include in final output
            available_columns = combined_df.columns.tolist()

            # 1. Always include pivot columns
            columns_to_keep = [col for col in self.pivot_columns if col in available_columns]

            # 2. Include target ANN columns if they exist
            for ann_col in self.target_ann_columns:
                if ann_col in available_columns:
                    columns_to_keep.append(ann_col)
                    print(f"  Including ANN column: {ann_col}")
                else:
                    print(f"  ANN column not found: {ann_col}")

            # 3. Include all INFO columns
            info_columns = [col for col in available_columns if col.startswith("INFO[")]
            columns_to_keep.extend(info_columns)
            print(f"  Including {len(info_columns)} INFO columns")

            # 4. Find family-specific AF columns
            family_af_columns = [col for col in available_columns if col.endswith('_AF')]
            columns_to_keep.extend(family_af_columns)
            if family_af_columns:
                print(f"  Including family-specific AF columns: {family_af_columns}")

            # 5. Include family and sample columns if they exist
            for meta_col in ['family', 'SAMPLE']:
                if meta_col in available_columns:
                    columns_to_keep.append(meta_col)

            # Remove duplicates while preserving order
            columns_to_keep = list(dict.fromkeys(columns_to_keep))

            # Filter dataframe to only keep selected columns
            filtered_df = combined_df[columns_to_keep].copy()
            print(f"Filtered to {len(columns_to_keep)} columns: {columns_to_keep}")

            # Determine aggregation columns (everything except pivot columns)
            agg_columns = [col for col in columns_to_keep if col not in self.pivot_columns]

            print(f"  Pivot columns: {self.pivot_columns}")
            print(f"  Aggregation columns: {agg_columns}")

            # Group by pivot columns
            grouped = filtered_df.groupby(self.pivot_columns)

            # Prepare aggregation dictionary
            agg_dict = {}
            for col in agg_columns:
                if col.endswith('_AF'):
                    # For family-specific AF columns, use family-specific aggregation
                    agg_dict[col] = self.aggregate_family_af_values
                else:
                    # For other columns, use standard aggregation
                    agg_dict[col] = self.aggregate_cross_file_values

            # Perform aggregation
            result_df = grouped.agg(agg_dict).reset_index()

            # Add row count showing number of rows combined per variant
            result_df['ROW_COUNT'] = grouped.size().values

            print(f"Final result shape: {result_df.shape}")

            # Save results
            self.save_results(result_df, output_path, export_tsv=export_tsv)

        except Exception as e:
            print(f"Error combining processed files: {e}")
            raise

    def create_family_af_columns(self, combined_df: pd.DataFrame, file_families: dict) -> pd.DataFrame:
        """
        Create properly named family-specific AF columns and ensure data integrity.

        Args:
            combined_df: Combined dataframe from all files
            file_families: Mapping of file paths to family names

        Returns:
            DataFrame with properly structured family AF columns
        """
        # Get all family names and sort them for consistent ordering
        all_families = sorted(set(file_families.values()))
        family_af_columns = [f"{family}_AF" for family in all_families]

        print(f"  Expected family AF columns: {family_af_columns}")

        # Create missing family AF columns with None values
        for expected_col in family_af_columns:
            if expected_col not in combined_df.columns:
                combined_df[expected_col] = None
                print(f"  Created missing column: {expected_col}")

        # Validate that each row has at most one non-null family AF value per variant
        af_columns_in_df = [col for col in combined_df.columns if col.endswith('_AF')]

        # Group by genomic coordinates and check AF distribution
        grouped = combined_df.groupby(self.pivot_columns)
        for name, group in grouped:
            af_counts = {}
            for af_col in af_columns_in_df:
                non_null_count = group[af_col].dropna().nunique()
                if non_null_count > 1:
                    print(f"Warning: Variant {name} has multiple AF values in {af_col}: {group[af_col].dropna().unique()}")
                af_counts[af_col] = non_null_count

        return combined_df

    def aggregate_family_af_values(self, series: pd.Series) -> any:
        """
        Aggregate family-specific AF values, ensuring only one value per family.
        Each family should contribute exactly one AF value or None.

        Args:
            series: pandas Series with family-specific AF values

        Returns:
            Single AF value for this family or None if no value exists
        """
        # Remove null values
        valid_values = series.dropna()

        if len(valid_values) == 0:
            return None
        elif len(valid_values) == 1:
            return valid_values.iloc[0]
        else:
            # Multiple values for the same family - this shouldn't happen
            # but if it does, take the first non-null value and warn
            unique_values = valid_values.unique()
            if len(unique_values) == 1:
                # All values are the same, return it
                return unique_values[0]
            else:
                # Different values for same family - potential data issue
                print(f"Warning: Multiple different AF values for same family: {unique_values.tolist()}")
                return valid_values.iloc[0]

    def aggregate_family_values(self, series: pd.Series) -> any:
        """
        Aggregate family-specific values (like FORMAT AF), preserving family distinctions.

        Args:
            series: pandas Series with family-specific values

        Returns:
            Aggregated value preserving family information
        """
        # Collect all non-null values
        valid_values = []

        for item in series.dropna():
            if isinstance(item, list):
                valid_values.extend(item)
            else:
                valid_values.append(item)

        if len(valid_values) == 0:
            return None
        elif len(valid_values) == 1:
            return valid_values[0]
        else:
            # For family-specific values, keep all unique values
            # This preserves different AF values across families
            unique_values = []
            for val in valid_values:
                if val not in unique_values:
                    unique_values.append(val)

            if len(unique_values) == 1:
                return unique_values[0]
            else:
                return unique_values

    def aggregate_cross_file_values(self, series: pd.Series) -> any:
        """
        Aggregate values across files, handling existing lists and new values.

        Args:
            series: pandas Series with values to aggregate (may contain lists)

        Returns:
            Aggregated value or list
        """
        # Collect all individual values
        all_values = []

        for item in series.dropna():
            if isinstance(item, list):
                all_values.extend(item)
            else:
                all_values.append(item)

        if len(all_values) == 0:
            return None
        elif len(all_values) == 1:
            return all_values[0]
        else:
            # Get unique values while preserving order
            unique_values = []
            for val in all_values:
                if val not in unique_values:
                    unique_values.append(val)

            if len(unique_values) == 1:
                return unique_values[0]
            else:
                return unique_values
    def process_zarr_files(self, processed_zarr_files: List[str], output_path: str, export_tsv: bool = False):
        """
        Main processing function for cross-file aggregation.
        Expects input files to already be processed by zarr_pivot_creator.py

        Args:
            processed_zarr_files: List of processed Zarr file paths
            output_path: Final output file path
            export_tsv: Whether to also export as TSV
        """
        print(f"Starting cross-file aggregation of {len(processed_zarr_files)} processed Zarr files...")
        print("Note: Individual files should already be processed with zarr_pivot_creator.py")

        # Validate all processed files
        valid_files = self.validate_processed_zarr_files(processed_zarr_files)

        if not valid_files:
            print("Error: No valid processed Zarr files found!")
            return

        # Perform cross-file aggregation
        print("\n=== CROSS-FILE AGGREGATION ===")
        self.combine_processed_zarr_files(valid_files, output_path, export_tsv=export_tsv)

        # Print summary
        print("\n=== PROCESSING SUMMARY ===")
        print(f"Input processed files: {len(valid_files)}")
        print(f"Final output: {output_path}")

    def close(self):
        """Clean up Dask client."""
        if self.client:
            self.client.close()

    def save_results(self, result_df: pd.DataFrame, output_path: str, export_tsv: bool = False):
        """
        Save aggregation results to Zarr file and optionally to TSV.

        Args:
            result_df: Aggregated results DataFrame
            output_path: Output Zarr file path
            export_tsv: Whether to also export as TSV file
        """
        output_path = Path(output_path)

        # Ensure output path has .zarr extension
        if not output_path.name.endswith('.zarr'):
            output_path = output_path.with_suffix('.zarr')

        try:
            # Prepare DataFrame for xarray conversion
            df_clean = result_df.copy()

            # Handle mixed types and ensure compatibility with xarray
            for col in df_clean.columns:
                if df_clean[col].dtype == 'object':
                    # Convert object columns to string to avoid xarray issues
                    df_clean[col] = df_clean[col].astype(str)
                    df_clean[col] = df_clean[col].replace('nan', None)

            # Convert to xarray Dataset
            ds = df_clean.to_xarray()

            # Add metadata about the aggregation process
            ds.attrs['processing_type'] = 'cross_file_aggregated'
            ds.attrs['pivot_columns'] = str(self.pivot_columns)
            ds.attrs['target_ann_columns'] = str(self.target_ann_columns)
            ds.attrs['total_variants'] = len(result_df)
            ds.attrs['aggregation_note'] = 'Variants aggregated across multiple files by genomic coordinates'

            # Save as Zarr
            ds.to_zarr(output_path, mode='w')
            print(f"✓ Results saved to Zarr: {output_path}")

            # Calculate Zarr size
            zarr_size_mb = sum(f.stat().st_size for f in output_path.rglob('*') if f.is_file()) / (1024 * 1024)
            print(f"  Zarr file size: {zarr_size_mb:.2f} MB")

            # Export TSV if requested
            if export_tsv:
                tsv_path = output_path.with_suffix('.tsv')
                result_df.to_csv(tsv_path, sep='\t', index=False, na_rep='.')
                print(f"✓ Also exported TSV: {tsv_path}")

                tsv_size_mb = tsv_path.stat().st_size / (1024 * 1024)
                print(f"  TSV file size: {tsv_size_mb:.2f} MB")

        except Exception as e:
            print(f"Error saving results: {e}")
            raise


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Cross-file Zarr aggregator - pivots on CHROM,POS,REF,ALT and aggregates specific ANN/INFO columns",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Combine specific processed zarr files (output as Zarr)
  python zarr_groupby_aggregator.py --zarr file1_processed.zarr file2_processed.zarr --output results.zarr

  # Combine with TSV export as well
  python zarr_groupby_aggregator.py --zarr file1_processed.zarr file2_processed.zarr --output results.zarr --export-tsv

  # Combine processed zarr files from a list
  python zarr_groupby_aggregator.py --file-list processed_zarr_files.txt --output results.zarr

  # Specify workers for SLURM
  python zarr_groupby_aggregator.py --zarr *.zarr --output results.zarr --workers 8

Output columns:
  - Pivot: CHROM, POS, REF, ALT
  - ANN: MAX_AF, VARIANT_CLASS, Feature_type, IMPACT, SYMBOL
  - All INFO columns
  - Family-specific AF: 1_AF, 2_AF, 3_AF, etc.
  - ROW_COUNT (number of rows combined)

Note: Input files should already be processed with zarr_pivot_creator.py
        """
    )

    # Input options (mutually exclusive)
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument('--zarr', nargs='+', help='One or more processed Zarr file paths')
    input_group.add_argument('--file-list', help='Text file containing processed Zarr file paths (one per line)')

    # Output options
    parser.add_argument('--output', required=True, help='Output file path (.zarr format, .tsv/.csv/.xlsx if using --export-tsv)')

    # Dask options
    parser.add_argument('--workers', type=int, help='Number of Dask workers (default: auto-detect all cores)')
    parser.add_argument('--chunk-size', default='1GB', help='Chunk size for Dask processing (default: 1GB)')

    # Output format options
    parser.add_argument('--export-tsv', action='store_true', help='Also export results as TSV file (in addition to Zarr)')

    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_arguments()

    print("=== Zarr Cross-File Aggregator ===")
    print(f"Chunk size: {args.chunk_size}")
    if args.workers:
        print(f"Workers: {args.workers}")
    else:
        print("Workers: Auto-detect (all available cores)")

    # Initialize aggregator
    aggregator = ZarrCrossFileAggregator(
        n_workers=args.workers,
        chunk_size=args.chunk_size
    )

    try:
        # Get list of processed Zarr files
        if args.zarr:
            zarr_files = args.zarr
        else:
            zarr_files = aggregator.load_zarr_files_list(args.file_list)

        if not zarr_files:
            print("Error: No processed Zarr files specified!")
            sys.exit(1)

        # Process files with cross-file aggregation
        aggregator.process_zarr_files(zarr_files, args.output, export_tsv=args.export_tsv)

    except KeyboardInterrupt:
        print("\nProcessing interrupted by user")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        aggregator.close()


if __name__ == "__main__":
    main()
