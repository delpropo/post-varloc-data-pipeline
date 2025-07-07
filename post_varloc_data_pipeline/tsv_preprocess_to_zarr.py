import os
import argparse
import pandas as pd


def arg_parser():
    parser = argparse.ArgumentParser(description='Preprocess TSV: rename headers, add family and filename columns, and convert to Zarr.')
    parser.add_argument('--input', type=str, help='TSV file name', required=True)
    parser.add_argument('--output', type=str, help='Output folder location', default=None)
    parser.add_argument('--rename_map', type=str, help='Optional CSV file with old,new header names', default=None)
    args = parser.parse_args()

    if args.output is None:
        args.output = os.path.dirname(args.input)
    if not os.path.exists(args.output):
        os.makedirs(args.output)
    return args


def get_family_and_filename(tsv_path):
    filename = os.path.basename(tsv_path)
    family = filename.split('.')[0]
    return family, filename


def load_rename_map(rename_map_path):
    rename_dict = {}
    if rename_map_path and os.path.exists(rename_map_path):
        import csv
        with open(rename_map_path) as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) == 2:
                    rename_dict[row[0]] = row[1]
    return rename_dict


def main():
    args = arg_parser()
    family, filename = get_family_and_filename(args.input)
    rename_dict = load_rename_map(args.rename_map)

    # Read TSV
    df = pd.read_csv(args.input, sep='\t', header=0, low_memory=False)

    # Rename columns if mapping provided
    if rename_dict:
        df.rename(columns=rename_dict, inplace=True)

    # Add family and filename columns
    df.insert(0, 'family', family)
    df.insert(1, 'filename', filename)

    # Save preprocessed TSV (optional, comment out if not needed)
    preproc_tsv = os.path.join(args.output, filename.replace('.tsv', '.preprocessed.tsv'))
    df.to_csv(preproc_tsv, sep='\t', index=False)

    # Convert to Zarr
    try:
        import xarray as xr
        df_xr = df.to_xarray()
        df_xr.attrs['column_order'] = df.columns.to_list()
        zarr_file = os.path.join(args.output, filename.replace('.tsv', '.zarr'))
        df_xr.to_zarr(zarr_file, mode='w')
    except ImportError:
        print('xarray is required for Zarr conversion. Install with: pip install xarray zarr')
        return

    print(f"Preprocessing complete. Zarr file saved to: {zarr_file}")

if __name__ == "__main__":
    main()
