"""
Jim DelProposto
2023-05-03

This script will convert a TSV file from dna-seq-varlociraptor to a zarr file
python tsv_to_zarr.py --input <TSV file name> --output <Output folder location>
This requires a substantial amount of memory.
I recommend at least 10x the size of the TSV file or there can be segmentation faults or other errors.
70G was used for a 7.9G TSV file which failed with a segmentation fault at 40G of memory.

also need to install openpyxl

Notes for running this script:
1.  This script should be used with the normalized probability file from dna-seq-varlociraptor
"""
import os
import argparse
import logging
import time
import pandas as pd
import sys

def arg_parser():
    parser = argparse.ArgumentParser(description='Convert a TSV file from dna-seq-varlociraptor to Zarr.')
    parser.add_argument('--input', type=str, help='TSV file name', required=True)
    parser.add_argument('--output', type=str, help='Output folder location', default=None)
    args = parser.parse_args()

    if args.output is None:
        args.output = os.path.dirname(args.input)
    if not os.path.exists(args.output):
        os.makedirs(args.output)

    return args


def pandas_to_saved_zarr(df, output_folder):
    """
    Convert a pandas DataFrame to a Zarr file and save it.

    Parameters:
    df (pd.DataFrame): The DataFrame to convert.
    output_folder (str): The folder where the Zarr file will be saved.

    Returns:
    str: The path to the saved Zarr file.
    """
    # extract the file name from the file_name column and replace the extension with zarr
    zarr_file_name = df["filename"].unique()
    assert len(zarr_file_name) == 1
    zarr_file_name = zarr_file_name[0].split(".")
    zarr_file_name[-1] = "zarr"
    zarr_file_name = ".".join(zarr_file_name)

    # Convert the DataFrame to an xarray Dataset
    df_xarray = df.to_xarray()

    # Add metadata attributes to the xarray Dataset
    df_xarray.attrs['column_order'] = df.columns.to_list()

    # convert xarray dataset and save to zarr in the specified output folder
    zarr_file_path = os.path.join(output_folder, zarr_file_name)
    df_xarray.to_zarr(zarr_file_path, mode='w')


def main():
    """
    Main function to convert a TSV file to a Zarr file format.

    This function performs the following steps:
    1. Parses command-line arguments to get the input TSV file and output folder location.
    2. Sets up logging to record the process.
    3. Loads the TSV file into a pandas DataFrame.
    4. Processes the DataFrame by adding and modifying columns.
    5. Converts the DataFrame to a Zarr file format and saves it to the specified output folder.

    Command-line Arguments:
    --input (str): The TSV file name to be processed.
    --output (str, optional): The output folder location. If not provided, the folder of the input file is used.

    Raises:
    SystemExit: If the input file is not a TSV file.

    Notes:
    - The function assumes that the TSV file contains specific columns such as 'reference allele', 'alternative allele',
      'chromosome', 'position', 'allele', and 'hgvsp'.
    - The function creates a unique identifier for each variant by combining chromosome, position, and allele.
    - The function categorizes variants into 'exon' or 'noncoding' based on the 'hgvsp' column.
    - The function logs the process and saves the log file with a timestamped name.
    """

    args = arg_parser()

    # Create a logs folder inside the output folder if it does not exist
    logs_folder = os.path.join(args.output, 'logs')
    if not os.path.exists(logs_folder):
        os.makedirs(logs_folder)

    # Create a string for the log file name with the current date and time
    log_file_name = time.strftime("%Y%m%d-%H%M%S") + '_log_tsv_to_zarr_' + os.path.basename(args.input).split('.')[0] + '.txt'
    args.logpath = os.path.join(logs_folder, log_file_name)

    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # Create a file handler
    handler = logging.FileHandler(args.logpath)
    handler.setLevel(logging.INFO)

    # Create a logging format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(handler)

    if args.input.endswith('.tsv') or args.input.endswith('.TSV'):
        logger.info('Loading TSV file')
        # Add the name of the file to the log file
        logger.info('Input file: ' + args.input)
        logger.info('Loading TSV file into dataframe')
        # Load TSV file into DataFrame
        df = pd.read_csv(args.input, sep='\t', header=0, low_memory=False)
        logger.info('Completed reading TSV file into dataframe')

        # Calculate the difference between the reference allele and the alternate allele
        df["ref_alt_diff"] = df["reference allele"].str.len() - df["alternative allele"].str.len()

        # Calculate the absolute value of the difference between the reference allele and the alternate allele
        df["abs_ref_alt_diff"] = df["ref_alt_diff"].abs()
        # Remove ref_alt_diff column
        df = df.drop(columns=["ref_alt_diff"])

        # Create a column where chromosome is joined to position and allele to create a unique id for each variant
        df["chr_pos_alt_allele"] = df["chromosome"] + "_" + df["position"].astype(str) + "_" + df["allele"]
        logger.info('Completed creating unique id for each variant')

        # Move ref_alt_diff and abs_ref_alt_diff to the first two columns
        df = df[["chr_pos_alt_allele", "abs_ref_alt_diff"] + [col for col in df.columns if col not in ["chr_pos_alt_allele", "abs_ref_alt_diff"]]]

        # Insert a column in the first position called var_type and set it to variant
        df.insert(0, 'var_type', 'variant')
        # Add a column called var_type to populate with exon or noncoding
        if df["hgvsp"].dtypes == 'float64' or df["hgvsp"].dtypes != 'O':
            # Create a new column called var_type and set it to noncoding
            df["var_type"] = "noncoding"
        else:
            # If df["hgvsp"] length is greater than 0, set var_type to exon
            df.loc[df["hgvsp"].str.len() > 0, "var_type"] = "exon"
            # If df["hgvsp"] length is 0, set var_type to noncoding
            df.loc[df["hgvsp"].str.len() == 0, "var_type"] = "noncoding"

        # Add the family name and filename to the dataframe
        df.insert(0, 'family', args.input.split('.')[0].split('/')[-1])
        df.insert(1, 'filename', args.input.split('/')[-1])

    else:
        logger.error('Input file must be a TSV file')
        sys.exit(1)

    # Convert DataFrame to Zarr and save
    pandas_to_saved_zarr(df, args.output)

    # Close the logging file
    logger.info('Completed process and closing log file')
    logger.removeHandler(handler)
    handler.close()

if __name__ == "__main__":
    main()
