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


def main() -> None:
    """
    Main function to preprocess TSV file and convert to Zarr format.

    Process:
        1. Parse command line arguments
        2. Read TSV file
        3. Apply custom renaming if provided
        4. Add family and filename columns
        5. Convert to Zarr format
    """
    args = arg_parser()
    family, filename = get_family_and_filename(args.input)
    rename_dict = load_rename_map(args.rename_map)

    # Read TSV file
    df = pd.read_csv(args.input, sep='\t', header=0, low_memory=False)

    # Apply custom column renaming if provided
    if rename_dict:
        df.rename(columns=rename_dict, inplace=True)

    # Add family and filename columns at the beginning
    df.insert(0, 'family', family)
    df.insert(1, 'filename', filename)

    # Convert to Zarr format
    try:
        import xarray as xr
        df_xr = df.to_xarray()
        df_xr.attrs['column_order'] = df.columns.to_list()
        zarr_file = os.path.join(args.output, filename.replace('.tsv', '.zarr'))
        df_xr.to_zarr(zarr_file, mode='w')
        print(f"Preprocessing complete. Zarr file saved to: {zarr_file}")
    except ImportError:
        print('ERROR: xarray is required for Zarr conversion. Install with: pip install xarray zarr')
        return


if __name__ == "__main__":
    main()
