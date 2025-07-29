#!/usr/bin/env python3
"""
Additional Zarr Filtering Tool

This program reads a specified Zarr file and applies gene filtering based on configuration.
It implements GENE and rsID FILTERING logic for genomic data.
The filtered file is saved in data/additional_filtering with 'add_filter' added to the filename before '.zarr'.

Usage:
    python additional_zarr_filtering.py --input data/processed/sample.zarr
    # Output: data/additional_filtering/sample_add_filter.zarr

Configuration:
    - Uses config.yaml for configuration
    - Gene filter file specified in [ADDITIONAL_ZARR_FILTERING] section: gene_filter = filename
    - Gene filter file can be TSV, CSV, or Excel (.xlsx/.xls)
    - Supports filtering by gene symbols (Symbol/Gene Symbol column) and/or rsIDs (rsID column)
    - Rows matching either gene symbols or rsIDs are kept (OR logic)
"""

import argparse
import pandas as pd
import xarray as xr
import warnings
from pathlib import Path
import sys

# Import our centralized configuration functions
from .config import get_config_value

# Suppress Zarr warnings
warnings.filterwarnings('ignore', message='.*vlen-utf8.*')
warnings.filterwarnings('ignore', message='.*StringDType.*')
warnings.filterwarnings('ignore', message='.*Consolidated metadata.*')


class AdditionalZarrFilter:
    """Apply gene filtering to Zarr files with same logic as zarr_pivot_creator.py"""

    def __init__(self, config_file=None):
        """Initialize with configuration file (uses config.yaml by default)."""
        self.config_file = config_file or "config.yaml"
        self.gene_filter_symbols = None
        self.gene_filter_rsids = None

    def get_output_dir_from_config(self):
        """Get output directory from configuration."""
        output_dir = get_config_value('ADDITIONAL_ZARR_FILTERING', 'OUTPUT_DIR',
                                    fallback="data/additional_filtering")
        print(f"Using output directory: {output_dir}")
        return output_dir

    def get_gene_filter_from_config(self):
        """Get gene filter file path from configuration."""
        gene_filter_file = get_config_value('ADDITIONAL_ZARR_FILTERING', 'GENE_FILTER')
        if gene_filter_file:
            print(f"Found gene filter in config: {gene_filter_file}")
        return gene_filter_file

    def load_gene_filter(self, gene_filter_file):
        """Load gene symbols and rsIDs from TSV/CSV/Excel file for filtering."""
        try:
            # Determine file type and read accordingly
            file_ext = Path(gene_filter_file).suffix.lower()

            if file_ext in ['.xlsx', '.xls']:
                # Read Excel file
                gene_df = pd.read_excel(gene_filter_file)
            elif file_ext == '.csv':
                # Read CSV file
                gene_df = pd.read_csv(gene_filter_file)
            else:
                # Default to TSV
                gene_df = pd.read_csv(gene_filter_file, sep='\t')

            print(f"Available columns in gene filter file: {list(gene_df.columns)}")

            # Initialize filter sets
            self.gene_filter_symbols = set()
            self.gene_filter_rsids = set()

            # Check for Symbol column (could be 'Symbol', 'Gene Symbol', etc.)
            symbol_column = None
            for col in gene_df.columns:
                if col.lower() in ['symbol', 'gene symbol', 'gene_symbol']:
                    symbol_column = col
                    break

            if symbol_column:
                # Extract unique gene symbols and remove any null values
                symbols = gene_df[symbol_column].dropna().unique()
                self.gene_filter_symbols = set(symbols)
                print(f"✓ Loaded {len(self.gene_filter_symbols)} gene symbols for filtering")
                print(f"  Example symbols: {list(self.gene_filter_symbols)[:5]}")

            # Check for rsID column
            rsid_column = None
            for col in gene_df.columns:
                if col.lower() in ['rsid', 'rs_id', 'rs id', 'rsid_paper']:
                    rsid_column = col
                    break

            if rsid_column:
                # Extract unique rsIDs and remove any null values
                rsids = gene_df[rsid_column].dropna().unique()
                # Clean rsIDs to ensure they start with 'rs'
                cleaned_rsids = []
                for rsid in rsids:
                    rsid_str = str(rsid).strip()
                    if rsid_str.startswith('rs'):
                        cleaned_rsids.append(rsid_str)

                self.gene_filter_rsids = set(cleaned_rsids)
                print(f"✓ Loaded {len(self.gene_filter_rsids)} rsIDs for filtering")
                print(f"  Example rsIDs: {list(self.gene_filter_rsids)[:5]}")

            # Ensure at least one filter type was found
            if not self.gene_filter_symbols and not self.gene_filter_rsids:
                available_columns = list(gene_df.columns)
                raise ValueError(f"Gene filter file must contain either a 'Symbol' column or 'rsID' column. "
                               f"Available columns: {available_columns}")

            total_filters = len(self.gene_filter_symbols) + len(self.gene_filter_rsids)
            print(f"✓ Total filter criteria loaded: {total_filters}")

        except Exception as e:
            print(f"Error loading gene filter file '{gene_filter_file}': {e}")
            raise

            print(f"✓ Loaded {len(self.gene_filter_symbols)} gene symbols for filtering")
            print(f"  Example genes: {list(self.gene_filter_symbols)[:5]}")

        except Exception as e:
            print(f"Error loading gene filter file '{gene_filter_file}': {e}")
            raise

    def apply_gene_filter(self, df):
        """Apply gene and rsID filtering."""
        if not self.gene_filter_symbols and not self.gene_filter_rsids:
            print("No gene or rsID filters loaded - returning original data")
            return df

        original_rows = len(df)
        masks = []

        # Apply gene symbol filtering if available
        if self.gene_filter_symbols and "ANN['SYMBOL']" in df.columns:
            def check_gene_match(symbols_str):
                if pd.isna(symbols_str) or symbols_str == '' or symbols_str == '.':
                    return False

                # ANN['SYMBOL'] can contain multiple symbols separated by various delimiters
                # Common delimiters: semicolon, comma, pipe, ampersand
                symbols = str(symbols_str).replace(';', '|').replace(',', '|').replace('&', '|').split('|')
                symbols = [s.strip() for s in symbols if s.strip()]

                # Check if any symbol matches our filter
                return any(symbol in self.gene_filter_symbols for symbol in symbols)

            gene_mask = df["ANN['SYMBOL']"].apply(check_gene_match)
            masks.append(gene_mask)
            symbol_matches = gene_mask.sum()
            print(f"🧬 GENE SYMBOL FILTERING: {symbol_matches:,} rows match gene symbols")

        # Apply rsID filtering if available
        if self.gene_filter_rsids and 'ID' in df.columns:
            def check_rsid_match(id_str):
                if pd.isna(id_str) or id_str == '' or id_str == '.':
                    return False

                # ID column can contain multiple IDs separated by various delimiters
                ids = str(id_str).replace(';', '|').replace(',', '|').replace('&', '|').split('|')
                ids = [id_val.strip() for id_val in ids if id_val.strip()]

                # Check if any ID matches our rsID filter
                return any(id_val in self.gene_filter_rsids for id_val in ids)

            rsid_mask = df['ID'].apply(check_rsid_match)
            masks.append(rsid_mask)
            rsid_matches = rsid_mask.sum()
            print(f"🆔 rsID FILTERING: {rsid_matches:,} rows match rsIDs")

        # Combine masks with OR logic (keep rows that match either filter)
        if masks:
            combined_mask = masks[0]
            for mask in masks[1:]:
                combined_mask = combined_mask | mask

            filtered_df = df[combined_mask].copy()
        else:
            print("Warning: No applicable columns found for filtering.")
            return df

        filtered_rows = len(filtered_df)
        removed_rows = original_rows - filtered_rows

        print("🎯 COMBINED FILTERING RESULTS:")
        print(f"   ORIGINAL: {original_rows:,} rows")
        print(f"   FILTERED: {filtered_rows:,} rows")
        print(f"   REMOVED:  {removed_rows:,} rows ({(removed_rows/original_rows)*100 if original_rows else 0:.1f}%)")

        return filtered_df

    def prepare_dataframe_for_xarray(self, df):
        """Prepare DataFrame for xarray conversion by ensuring consistent data types."""
        df_clean = df.copy()

        for col in df_clean.columns:
            # Convert object columns with mixed types to string
            if df_clean[col].dtype == 'object':
                df_clean[col] = df_clean[col].astype(str)

        return df_clean

    def process_zarr_file(self, input_path, output_path=None):
        """Process a Zarr file with gene filtering."""
        print(f"Processing Zarr file: {input_path}")

        # Validate input file
        input_path = Path(input_path)
        if not input_path.exists():
            raise FileNotFoundError(f"Input Zarr file not found: {input_path}")

        # Get gene filter file from config
        gene_filter_file = self.get_gene_filter_from_config()

        # Load gene filter if available
        if gene_filter_file:
            gene_filter_path = Path(gene_filter_file)
            if not gene_filter_path.exists():
                raise FileNotFoundError(f"Gene filter file not found: {gene_filter_file}")
            self.load_gene_filter(gene_filter_file)
        else:
            print("No gene filter specified - processing without gene filtering")

        # Load the Zarr file
        print("Loading Zarr data...")
        ds = xr.open_zarr(str(input_path))
        df = ds.to_dataframe().reset_index()

        print(f"Original data shape: {df.shape}")

        # Apply gene filtering
        filtered_df = self.apply_gene_filter(df)        # Determine output path
        if output_path is None:
            # Get output directory from config
            output_dir_str = self.get_output_dir_from_config()
            output_dir = Path(output_dir_str)
            output_dir.mkdir(parents=True, exist_ok=True)

            # Add 'add_filter' before '.zarr' extension
            input_stem = input_path.stem
            if input_stem.endswith('.zarr'):
                # Handle case where stem includes .zarr (shouldn't happen but just in case)
                base_name = input_stem[:-5]  # Remove .zarr
            else:
                base_name = input_stem

            output_filename = f"{base_name}_add_filter.zarr"
            output_path = output_dir / output_filename
        else:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)

        print(f"Output path: {output_path}")

        # Prepare DataFrame for xarray conversion
        print("Preparing data for Zarr conversion...")
        df_clean = self.prepare_dataframe_for_xarray(filtered_df)

        # Convert to xarray
        ds_filtered = df_clean.to_xarray()

        # Add metadata
        ds_filtered.attrs['original_file'] = str(input_path)
        ds_filtered.attrs['processing_type'] = 'gene_filtered'
        ds_filtered.attrs['original_rows'] = len(df)
        ds_filtered.attrs['filtered_rows'] = len(filtered_df)

        if gene_filter_file:
            ds_filtered.attrs['gene_filter_file'] = str(gene_filter_file)
            gene_count = len(self.gene_filter_symbols) if self.gene_filter_symbols else 0
            rsid_count = len(self.gene_filter_rsids) if self.gene_filter_rsids else 0
            ds_filtered.attrs['gene_filter_count'] = gene_count
            ds_filtered.attrs['rsid_filter_count'] = rsid_count

        # Add column information
        column_dtypes = {col: str(df_clean[col].dtype) for col in df_clean.columns}
        ds_filtered.attrs['column_dtypes'] = str(column_dtypes)

        # Save filtered Zarr
        print("Saving filtered Zarr file...")
        ds_filtered.to_zarr(str(output_path), mode='w')

        print(f"✓ Successfully saved filtered Zarr: {output_path}")
        print(f"  Original rows: {len(df):,}")
        print(f"  Filtered rows: {len(filtered_df):,}")

        return str(output_path), filtered_df


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Apply gene and rsID filtering to Zarr files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Filter using gene list from config file
  python additional_zarr_filtering.py --input data/processed/sample.zarr

  # Export as both Zarr and TSV
  python additional_zarr_filtering.py --input sample.zarr --export-tsv

  # Specify custom TSV output location
  python additional_zarr_filtering.py --input sample.zarr --export-tsv --tsv-output custom_output.tsv

  # Specify output locations for both formats
  python additional_zarr_filtering.py --input sample.zarr --output custom.zarr --export-tsv --tsv-output custom.tsv

Output:
  - Default Zarr: data/additional_filtering/{input_name}_add_filter.zarr
  - Default TSV: same as Zarr output with .tsv extension
  - Custom: specified output paths

Configuration:
  - Uses config.yaml for configuration
  - Gene filter file must be specified in [ADDITIONAL_ZARR_FILTERING] section: GENE_FILTER = filename
  - Gene filter file supports TSV, CSV, and Excel formats
  - Filters by gene symbols (Symbol/Gene Symbol column) and/or rsIDs (rsID column)
  - Uses OR logic: keeps rows matching either gene symbols or rsIDs
        """
    )

    parser.add_argument('--input', '-i', required=True, help='Input Zarr file path')
    parser.add_argument('--config', '-c', help='Configuration file (uses config.yaml by default)')
    parser.add_argument('--output', '-o', help='Output Zarr file path (default: data/additional_filtering/{input_name}_add_filter.zarr)')
    parser.add_argument('--export-tsv', action='store_true', help='Also export processed data as TSV file')
    parser.add_argument('--tsv-output', help='TSV output file path (default: same as zarr output with .tsv extension)')

    args = parser.parse_args()

    try:
        # Initialize filter
        filter_processor = AdditionalZarrFilter(args.config)

        # Process the file
        output_path, processed_df = filter_processor.process_zarr_file(
            input_path=args.input,
            output_path=args.output
        )

        # Export TSV if requested
        if args.export_tsv:
            # Determine column order from config if available
            column_order_str = get_config_value('ADDITIONAL_ZARR_FILTERING', 'COLUMN_ORDER')
            column_order = None
            if column_order_str:
                column_order = [col.strip() for col in column_order_str.split(',')]

            # Determine TSV output path
            if args.tsv_output:
                tsv_path = args.tsv_output
            else:
                # Use same base name as Zarr output but with .tsv extension
                zarr_path = Path(output_path)
                tsv_path = str(zarr_path.parent / f"{zarr_path.stem}.tsv")

            print(f"Exporting processed data to TSV: {tsv_path}")

            # Convert nullable types to standard types for TSV export
            export_df = processed_df.copy()
            for col in export_df.columns:
                if export_df[col].dtype.name in ['Int64', 'boolean']:
                    # Convert nullable types to standard types, replacing NaN with appropriate values
                    if export_df[col].dtype.name == 'Int64':
                        export_df[col] = export_df[col].astype('float64')  # Will show NaN for missing
                    elif export_df[col].dtype.name == 'boolean':
                        export_df[col] = export_df[col].astype('object')  # Will show None for missing

            # Reorder columns if column_order is specified and valid
            if column_order:
                # Only use columns that exist in the DataFrame
                valid_order = [col for col in column_order if col in export_df.columns]
                remaining_cols = [col for col in export_df.columns if col not in valid_order]
                remaining_cols_sorted = sorted(remaining_cols)
                export_df = export_df[valid_order + remaining_cols_sorted]

            # Save to TSV
            export_df.to_csv(tsv_path, sep='\t', index=False, na_rep='.')
            print(f"✓ Successfully exported TSV: {tsv_path}")

            # Report file sizes
            zarr_size_mb = sum(f.stat().st_size for f in Path(output_path).rglob('*') if f.is_file()) / (1024 * 1024)
            tsv_size_mb = Path(tsv_path).stat().st_size / (1024 * 1024)
            print("\n📁 FILE SIZES:")
            print(f"   Zarr file: {zarr_size_mb:.1f} MB")
            print(f"   TSV file: {tsv_size_mb:.1f} MB")

        print("\n✓ Processing complete!")
        print(f"  Input:  {args.input}")
        print(f"  Output: {output_path}")

    except Exception as e:
        print(f"✗ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()