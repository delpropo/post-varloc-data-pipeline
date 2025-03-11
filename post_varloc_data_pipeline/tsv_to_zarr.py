"""
Jim DelProposto
2023-05-03

This script will convert a TSV file from dna-seq-varlociraptor to a zarr file
python tsv_to_zarr.py --input <TSV file name>
This requires a substantial amount of memory.
I recommend at least 10x the size of the TSV file or there can be segmentation faults or other errors.
70G was used for a 7.9G TSV file which faild with a segmentation fault at 40G of memory.


also need to install openpyxl
mamba install -c conda-forge -c anaconda -c bioconda openpyxl

Notes for running this script:
1.  This script should be used with the normalized probability file from dna-seq-varlociraptor

"""
import xarray as xr
# import zarr
import sys
# import os
import argparse
import logging
import time
# import numpy as np
import pandas as pd



def pandas_to_saved_zarr(df):
    """
    extract the file name from the filename column
    convert pandas to xarray to zarr and save to file
    """
    # extract the file name from the file_name column and replace the extension with zarr
    zarr_file_name = df["filename"].unique()
    assert len(zarr_file_name) == 1
    zarr_file_name = zarr_file_name[0].split(".")
    zarr_file_name[-1] = "zarr"
    zarr_file_name = ".".join(zarr_file_name)
    df_xarray = df.to_xarray()

    # add metadata attributes to the dataframe
    df_xarray.attrs['column_order'] = df.columns.to_list()

    # convert xarray dataset and save to zarr
    return df_xarray.to_zarr(zarr_file_name, mode='w')

def main():
    print("TSV_to_zarr:  Filename will be used as the zarr file name")


    # accept a vcf file name as input
    parser = argparse.ArgumentParser(description='Filter a TSV file from dna-seq-varlociraptor')
    parser.add_argument('--input', type=str, help='TSV file name')
    args = parser.parse_args()

    # set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logger = logging.getLogger(__name__)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # create a string for the log file name with the current date and time
    log_file_name = 'log_tsv_to_zarr_' + time.strftime("%Y%m%d-%H%M%S") + '.txt'
    # create a file handler
    handler = logging.FileHandler(log_file_name )
    handler.setLevel(logging.INFO)
    # create a logging format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(handler)



    if args.input.endswith('.tsv') or args.input.endswith('.TSV'):
        logger.info('Loading TSV file')
        # add the name of the file to the log file
        logger.info('Input file:  ' + args.input)
        logger.info('Loading TSV file into dataframe')
        # there were issue with getting this to work.  I simply increased the amount of memory to ~10x the size of the tsv file and it worked
        df = pd.read_csv(args.input, sep='\t', header=0, low_memory=False)


        # calculate the difference between the reference allele and the alternate allele
        df["ref_alt_diff"] = df["reference allele"].str.len() - df["alternative allele"].str.len()

        # calculate the absolute value of the difference between the reference allele and the alternate allele
        df["abs_ref_alt_diff"] = df["ref_alt_diff"].abs()
        # remove ref_alt_diff column
        df = df.drop(columns=["ref_alt_diff"])


        # create a column where chromosome is joined to position and allele.  This create a unique id for each variant
        df["chr_pos_alt_allele"] = df["chromosome"] + "_" + df["position"].astype(str) + "_" + df["allele"]




        # move ref_alt_diff and abs_ref_alt_diff to the first two columns
        df = df[["chr_pos_alt_allele", "abs_ref_alt_diff"] + [col for col in df.columns if col not in ["chr_pos_alt_allele", "abs_ref_alt_diff"]]]


        # insert a column in first position called data_type and set it to variant
        df.insert(0, 'var_type', 'variant')
        # add a column called var_type to populate with exon or noncoding
        if df["hgvsp"].dtypes == 'float64' or df["hgvsp"].dtypes != 'O':
            # create a new column called var_type and set it to intron
            df["var_type"] = "noncoding"
        else:
            # if df["hgvsp"] length is greater than 0, set var_type to exon
            df.loc[df["hgvsp"].str.len() > 0, "var_type"] = "exon"
            # if df["hgvsp"] length is 0, set var_type to intron
            df.loc[df["hgvsp"].str.len() == 0, "var_type"] = "noncoding"


        # add the family name and filename to the dataframe
        df.insert(0, 'family', args.input.split('.')[0].split('/')[-1])
        df.insert(1, 'filename', args.input.split('/')[-1])

    else:
        logger.error('Input file must be a TSV file')
        sys.exit(1)


    df_zarr = pandas_to_saved_zarr(df)

    # close the logging file
    logger.info('Completed process and closing log file')
    logger.removeHandler(handler)
    handler.close()

if __name__ == "__main__":
    main()
