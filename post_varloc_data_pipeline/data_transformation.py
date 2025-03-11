"""
This script processes genomic data stored in Zarr format. It performs various filtering and transformation operations
and saves the results as a TSV file.

Usage:
    python data_transformation.py --input <input_zarr_folder> --output <output_folder>

Arguments:
    --input: Path to the input Zarr folder.
    --output: Path to the output folder where the results will be saved. If not provided, the output will be saved in the same directory as the input.

Example:
    python data_transformation.py --input /path/to/zarr_folder --output /path/to/output_folder

Description:
    1. Load the Zarr file and convert it to an xarray dataset.
    2. Drop unnecessary columns.
    3. Apply various filters such as impact, maximum allele frequency, etc.
    4. Group the data by genome location.
    5. Save the transformed data as a TSV file.

Tests:
    def test_load_zarr_to_xarray(xarray_dataset):
    def test_xarray_filter_max_af(xarray_dataset):
    def test_xarray_filter_search_allele_frequency_issues(xarray_dataset):
    def test_xarray_get_allele_frequency_counts(xarray_dataset):

TODO:
    def xarray_filter_homozygous(xr_df: xr.Dataset):
    def xarray_filter_impact(xr_df: xr.Dataset, impact: list = ["HIGH", "MODERATE"]):
    def xarray_filter_loftool_score(xr_df: xr.Dataset, loftool_score_ceiling: float = 0.5):
    def xarray_filter_search_allele_frequency_issues(xr_df: xr.Dataset):
    def xarray_get_allele_frequency_counts(xr_df: xr.Dataset):
    def xarray_gene_multivariant_filter(xr_df: xr.Dataset):
    def xarray_groupby_position(xr_df: xr.Dataset):
"""
import xarray as xr
import os
import argparse
import logging
import time
import pandas as pd
import datetime
import configparser

def argument_parser() -> argparse.Namespace:
    """
    parse the arguments
    args: input file name, output file name
    """
    parser = argparse.ArgumentParser(description='transform or filter a file')
    parser.add_argument('--input', type=str, help='zarr folder name or names')
    parser.add_argument('--output', type=str, help='Output folder location', default=None)
    parser.add_argument('--config', type=str, help='Path to config.ini file', default=None)
    args = parser.parse_args()

    if not args.input.endswith('.zarr'):
        raise ValueError("Input file must end with .zarr")

    if args.output is None:
        args.output = os.path.dirname(args.input)
        if not os.path.exists(args.output):
            os.makedirs(args.output)
    else:
        if not os.path.isdir(args.output):
            raise ValueError("Output path must be a directory")

    if args.config is None:
        args.config = os.path.join(os.path.dirname(args.input), 'config.ini')
        if not os.path.isfile(args.config):
            raise ValueError("Config file not found")
    else:
        if not os.path.isfile(args.config) or not args.config.endswith('config.ini'):
            raise ValueError("Config file must be a valid file named config.ini")
        args.config = os.path.abspath(args.config)

    return args

config = configparser.ConfigParser()
args = argument_parser()
config.read(args.config)


def open_zarr_to_xarray(zarr_folder: str, drop_variables=[]) -> xr.Dataset:
    """
    load a zarr file and convert to xarray
    perform a check to see if the xarray has the attribute column_order
    return the xarray
    zarr_folder: path to the zarr folder
    """
    # loading the zarr file as an xarray
    try:
        xr_df = xr.open_zarr(zarr_folder, drop_variables=drop_variables)
    except:
        raise ValueError("The zarr file is not valid")

     # perform check to see if the xarray has the attribute column_order
    if "column_order" not in xr_df.attrs:
        raise ValueError("The xarray does not have the attribute column_order")

    # set choromosome as a string
    xr_df["chromosome"] = xr_df["chromosome"].astype(str)
    # set position as an integer
    xr_df["position"] = xr_df["position"].astype(int)
    # set family as a string
    xr_df["family"] = xr_df["family"].astype(str)


    return xr_df

def xarray_filter_var_type(xr_df: xr.Dataset, filter_type: str = config['DEFAULT']['filter_type']) -> xr.Dataset:
    """
    filter the xarray dataset for var_type
    default is exon for protein coding.  noncoding is for non-protein coding
    xr_df: xarray dataset
    """
    # raise an error if filter_type is not exon or noncoding
    if filter_type not in ["exon", "noncoding", "variant"]:
        raise ValueError("filter_type must be exon, noncoding, or variant")


    if filter_type == "noncoding":
        mask = (xr_df["var_type"] == "noncoding").compute()
    elif filter_type == "exon":
        mask = (xr_df["var_type"] == "exon").compute()
    else:
        raise ValueError("error in var_type where it is not exon or noncoding")

    return xr_df.where(mask, drop=True)

# TODO fix this function
def xarray_filter_max_af(xr_df: xr.Dataset, max_af_ceiling: float = float(config['DEFAULT']['max_af_ceiling'])) -> xr.Dataset:
    """
    filter the xarray dataset for max_af.  Keeps variants without a value for max_af
    xr_df: xarray dataset
    max_af: maximum allele frequency
    """
    # mask if max_af is less than the max_af_ceiling or if max_af is nan
    mask_max_af = (xr_df["max_af"] < max_af_ceiling).compute() | (xr_df["max_af"].isnull().compute())


    return xr_df.where(mask_max_af, drop=True)

# impact of the variant
def xarray_filter_impact(xr_df: xr.Dataset, impact: list = config['DEFAULT']['impact'].split(',')) -> xr.Dataset:
    """
    filter the xarray dataset for impact
    xr_df: xarray dataset
    impact: impact of the variant.  Default is ["HIGH", "MODERATE"].  Other options are "LOW", "MODIFIER"
    """
    # create filter for all rows where "impact" is in the list impact
    mask_impact = (xr_df["impact"].isin(impact)).compute()
    return xr_df.where(mask_impact, drop=True)





def logger_setup(args, log_file_prefix: str = config['DEFAULT']['log_file_prefix']) -> logging.Logger:
    """
    set up the logger
    logger: the logger
    """
    logs_folder = os.path.join(args.output, 'logs')
    if not os.path.exists(logs_folder):
        os.makedirs(logs_folder)

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logger = logging.getLogger(__name__)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    log_file_name = os.path.join(logs_folder, time.strftime("%Y%m%d-%H%M%S") + f'_{log_file_prefix}.txt')
    handler = logging.FileHandler(log_file_name)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # add args to the log file
    logger.info(f"input: {args.input}")

    return logger


def drop_columns(xr_df: xr.Dataset) -> xr.Dataset:
    """
    drop columns that appear to be useless
    """
    columns = list(xr_df.data_vars.keys())
    drop_columns = config['DROP_COLUMNS']['columns'].split(',')

    for drop in drop_columns:
        if drop in columns:
            xr_df = xr_df.drop_vars(drop)

    return xr_df

def drop_long_observation_columns(xr_df: xr.Dataset) -> xr.Dataset:
    """
    The long observation columns contain complete information about the variant.
    Normal processing does not require this information and it takes up a lot of space.
    This should be run as the first step in the process
    remove all columns that contain ": observations"
    """
    columns = list(xr_df.data_vars.keys())
    long_observation_columns = [column for column in columns if ": observations" in column and "short" not in column]
    xr_df = xr_df.drop_vars(long_observation_columns)
    return xr_df



def groupby_genome_location(xr_df: xr.Dataset) -> pd.DataFrame:
    """
    groupby genome location and additional values which need to be unique
    ["chromosome", "position", "allele", "family"]

    """


    # get a list of the columns
    columns = list(xr_df.data_vars.keys())
    groupby_list = ["chromosome", "position", "allele", "family",]

    # fewer took less time
    # additional columns to groupby
    # abs_ref_alt_diff
    additional = [
        "abs_ref_alt_diff",
        "filename",
        "alternative allele",
        "chr_pos_alt_allele",
        "clinical significance",
        "existing_variation",
        "hgvsg",
        "max_af",
        "variant_class",
        # "var_type", # this is not unique in case of variants where they are both exon
        ]


    unique_value_columns = [
                            ": allele frequency",
                            ": read depth",
                            "prob:"
                            "observations",]

    for column in columns:
        if column in additional:
            groupby_list.append(column)
        if any([unique in column for unique in unique_value_columns]):
            groupby_list.append(column)

    groupby_df = xr_df.to_pandas()
    groupby_df = groupby_df.groupby(groupby_list, sort=False, dropna=False)
    groupby_df_agg = groupby_df.agg(['unique'])
    groupby_df_agg = groupby_df_agg.reset_index(drop=False)
    groupby_df_agg.columns = [''.join(col).strip() for col in groupby_df_agg.columns.values]

    return groupby_df_agg


def main(args):

    logger = logger_setup(args)

    if args.output:
        output_file = args.output
    else:
        output_file = "output"



    if not output_file.endswith('.tsv'):
        output_file = output_file + '.tsv'

    input_files = args.input.split(' ')

    for input_file in input_files:
        if not input_file.endswith('.zarr'):
            logger.error('Input file must be a zarr folder')
            logger.info(f"Input file: {input_file} is invalid.")
            exit("error: Input file must be a zarr folder")
        else:
            logger.info('confirmed zarr files')
            logger.info(f"input files: {input_files}\noutput file: {output_file}")

    logger.info(f"Config file path: {os.path.abspath(args.config)}")


    logger.info("completed input. now load the zarr folder")

    logger.info("the time is now: %s", datetime.datetime.now())
    xr_df = open_zarr_to_xarray(input_files[0])
    logger.info("the time is now: %s", datetime.datetime.now())
    logger.info("completed loading zarr file")

    xr_df = drop_columns(xr_df)

    logger.info("dropping the long observation column. The time is now: %s", datetime.datetime.now())
    xr_df = drop_long_observation_columns(xr_df)
    logger.info("complete dropping long observation column. The time is now: %s", datetime.datetime.now())

    logger.info("xr_df size before xarray_filter_impact filter: %s", xr_df.info)
    logger.info("running impact filter. the time is now: %s", datetime.datetime.now())
    xr_df = xarray_filter_impact(xr_df, impact=["HIGH", "MODERATE", "LOW", "MODIFIER"])
    logger.info("xr_df size: %s", xr_df.info)
    logger.info("running max_af filter. the time is now: %s", datetime.datetime.now())
    xr_df = xarray_filter_max_af(xr_df, max_af_ceiling=0.1)
    logger.info("complete max_af filter. the time is now: %s", datetime.datetime.now())
    logger.info("xr_df size: %s", xr_df.info)

    logger.info("started groupby_genome_location function %s", datetime.datetime.now())
    xr_df = groupby_genome_location(xr_df)
    logger.info("xr_df size after groupby: %s", xr_df.info)

    columns = xr_df.columns
    columns = [column.replace("unique", "") for column in columns]
    xr_df.columns = columns

    file_name = xr_df["filename"].values[0]
    logger.info("xr_df size after groupby: %s", xr_df.info)
    final_file_name = os.path.join(args.output, f"genome_location_groupby_{file_name}")
    xr_df.to_csv(final_file_name, sep="\t")
    logger.info("completed groupby_genome_location function %s", datetime.datetime.now())

    logger.info(f"number of rows in xr_df at end: {xr_df.info}")
    logger.info('Completed zarr transformations and closing log file')
    # save the log file
    for handler in logger.handlers:
        handler.close()
        logger.removeHandler(handler)

if __name__ == "__main__":
    main(args)
