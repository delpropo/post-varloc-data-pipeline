import os
import argparse
import csv
from typing import Dict, Tuple
import pandas as pd
import warnings

# Suppress Zarr v3 specification warnings
warnings.filterwarnings('ignore', message='.*vlen-utf8.*not part in the Zarr format 3 specification.*')
warnings.filterwarnings('ignore', message='.*StringDType.*not part in the Zarr format 3 specification.*')
warnings.filterwarnings('ignore', message='.*Consolidated metadata.*not part in the Zarr format 3 specification.*')


def arg_parser() -> argparse.Namespace:
    """
    Parse command line arguments for TSV preprocessing.

    Returns:
        argparse.Namespace: Parsed command line arguments with validated output directory.
    """
    parser = argparse.ArgumentParser(
        description='Preprocess TSV: clean headers, add family and filename columns, and convert to Zarr.'
    )
    parser.add_argument('--input', type=str, help='TSV file name', required=True)
    parser.add_argument('--output', type=str, help='Output folder location', default=None)
    parser.add_argument('--rename_map', type=str, help='Optional CSV file with old,new header names', default=None)
    parser.add_argument('--dtype_file', type=str, help='CSV file with column data types',
                       default='references/combined_ann_dtypes.csv')
    parser.add_argument('--skip_dtypes', action='store_true', help='Skip applying data types from dtype_file')

    args = parser.parse_args()

    if args.output is None:
        args.output = os.path.dirname(args.input)
    if not os.path.exists(args.output):
        os.makedirs(args.output)

    return args


def get_family_and_filename(tsv_path: str) -> Tuple[str, str]:
    """
    Extract family name and filename from TSV file path.

    Args:
        tsv_path (str): Path to the TSV file.

    Returns:
        Tuple[str, str]: Family name (first part before '.') and full filename.
    """
    filename = os.path.basename(tsv_path)
    family = filename.split('.')[0]
    return family, filename


def load_rename_map(rename_map_path: str) -> Dict[str, str]:
    """
    Load column rename mapping from CSV file.

    Args:
        rename_map_path (str): Path to CSV file containing old,new column name pairs.

    Returns:
        Dict[str, str]: Dictionary mapping old column names to new column names.
    """
    rename_dict = {}
    if rename_map_path and os.path.exists(rename_map_path):
        with open(rename_map_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) == 2:
                    rename_dict[row[0]] = row[1]
    return rename_dict


def load_dtype_mapping(dtype_file: str = "references/combined_ann_dtypes.csv") -> Dict[str, str]:
    """
    Load data type mapping from CSV file.

    Args:
        dtype_file (str): Path to CSV file containing column_name,dtype pairs.

    Returns:
        Dict[str, str]: Dictionary mapping column names to pandas data types.
    """
    dtype_dict = {}
    if os.path.exists(dtype_file):
        print(f"Loading data types from: {dtype_file}")
        with open(dtype_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                column_name = row.get('column_name', '').strip()
                dtype = row.get('dtype', '').strip()
                if column_name and dtype:
                    dtype_dict[column_name] = dtype
        print(f"Loaded {len(dtype_dict)} data type mappings")
    else:
        print(f"Warning: Data type file not found: {dtype_file}")

    return dtype_dict


def convert_pandas_dtypes(dtype_dict: Dict[str, str]) -> Dict[str, str]:
    """
    Convert data type strings to zarr-compatible dtypes.

    Args:
        dtype_dict (Dict[str, str]): Dictionary of column names to dtype strings.

    Returns:
        Dict[str, str]: Dictionary with zarr-compatible dtype specifications.
    """
    pandas_dtypes = {}

    for column, dtype in dtype_dict.items():
        if dtype == 'int64':
            # Use standard int64 for zarr compatibility (missing values will be filled)
            pandas_dtypes[column] = 'int64'
        elif dtype == 'float64':
            # float64 naturally handles NaN and is zarr compatible
            pandas_dtypes[column] = 'float64'
        elif dtype in ['boolean', 'bool']:
            # Use standard bool for zarr compatibility
            pandas_dtypes[column] = 'bool'
        elif dtype == 'string':
            # Use object type for string data in zarr
            pandas_dtypes[column] = 'object'
        else:
            # Default to object for unknown types
            pandas_dtypes[column] = 'object'

    return pandas_dtypes


def apply_dtypes_to_dataframe(df: pd.DataFrame, dtype_dict: Dict[str, str]) -> pd.DataFrame:
    """
    Apply data types to DataFrame columns, handling missing/blank values for zarr compatibility.

    Args:
        df (pd.DataFrame): Input DataFrame.
        dtype_dict (Dict[str, str]): Dictionary mapping column names to pandas dtypes.

    Returns:
        pd.DataFrame: DataFrame with corrected data types.
    """
    pandas_dtypes = convert_pandas_dtypes(dtype_dict)

    converted_count = 0
    for column, target_dtype in pandas_dtypes.items():
        if column in df.columns:
            try:
                # Handle common missing value representations
                if target_dtype in ['int64', 'float64', 'bool']:
                    # Replace common missing value representations with appropriate values
                    if target_dtype == 'int64':
                        # For integers, replace missing with 0 or a sentinel value
                        df[column] = df[column].replace(['', '.', 'NA', 'NULL', 'null'], 0)
                        # Convert to numeric first, then to int
                        df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0).astype('int64')
                    elif target_dtype == 'float64':
                        # For floats, replace missing with NaN (which float64 handles naturally)
                        df[column] = df[column].replace(['', '.', 'NA', 'NULL', 'null'], pd.NA)
                        df[column] = pd.to_numeric(df[column], errors='coerce')
                    elif target_dtype == 'bool':
                        # For booleans, replace missing with False
                        df[column] = df[column].replace(['', '.', 'NA', 'NULL', 'null'], False)
                        df[column] = df[column].astype('bool')
                else:
                    # For object/string types, keep as is
                    df[column] = df[column].astype(target_dtype)

                converted_count += 1

            except (ValueError, TypeError) as e:
                print(f"Warning: Could not convert column '{column}' to {target_dtype}: {e}")
                # Keep as object if conversion fails
                df[column] = df[column].astype('object')

    print(f"Successfully applied data types to {converted_count} columns")
    return df


def main() -> None:
    """
    Main function to preprocess TSV file and convert to Zarr format.

    Process:
        1. Parse command line arguments
        2. Read TSV file
        3. Apply custom renaming if provided
        4. Apply data types from dtype file
        5. Add family and filename columns
        6. Convert to Zarr format
    """
    args = arg_parser()
    family, filename = get_family_and_filename(args.input)
    rename_dict = load_rename_map(args.rename_map)

    print(f"Reading TSV file: {args.input}")

    # Read TSV file
    df = pd.read_csv(args.input, sep='\t', header=0, low_memory=False)
    original_shape = df.shape
    print(f"Original data shape: {original_shape}")

    # Apply custom column renaming if provided
    if rename_dict:
        df.rename(columns=rename_dict, inplace=True)
        print(f"Applied {len(rename_dict)} column renames")

    # Apply data types if not skipped
    if not args.skip_dtypes:
        dtype_dict = load_dtype_mapping(args.dtype_file)
        if dtype_dict:
            print("Applying data types...")
            df = apply_dtypes_to_dataframe(df, dtype_dict)

            # Report memory usage improvement
            print("Data types applied. Checking memory usage...")
            memory_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
            print(f"DataFrame memory usage: {memory_mb:.1f} MB")

    # Add family and filename columns at the beginning
    df.insert(0, 'family', family)
    df.insert(1, 'filename', filename)

    final_shape = df.shape
    print(f"Final data shape: {final_shape}")

    # Convert to Zarr format
    try:
        import xarray as xr
        df_xr = df.to_xarray()
        df_xr.attrs['column_order'] = df.columns.to_list()
        df_xr.attrs['original_shape'] = original_shape
        df_xr.attrs['final_shape'] = final_shape
        df_xr.attrs['family'] = family
        df_xr.attrs['source_file'] = args.input

        zarr_file = os.path.join(args.output, filename.replace('.tsv', '.zarr'))
        df_xr.to_zarr(zarr_file, mode='w')

        print("\n✓ Preprocessing complete!")
        print(f"✓ Zarr file saved to: {zarr_file}")

        # Report final statistics
        print("\n📊 PROCESSING SUMMARY:")
        print(f"   Input file: {args.input}")
        print(f"   Output file: {zarr_file}")
        print(f"   Rows: {final_shape[0]:,}")
        print(f"   Columns: {final_shape[1]:,}")
        if not args.skip_dtypes and dtype_dict:
            print(f"   Data types applied: {len([c for c in dtype_dict.keys() if c in df.columns])}")

    except ImportError:
        print('ERROR: xarray is required for Zarr conversion. Install with: pip install xarray zarr')
        return


if __name__ == "__main__":
    main()
