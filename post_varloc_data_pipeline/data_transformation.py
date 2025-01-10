"""
import zarr file or files
convert to pandas dataframe
filter and transform the data
save as excel file






tests

def test_load_zarr_to_xarray(xarray_dataset):
def test_xarray_filter_max_af(xarray_dataset):
def test_xarray_filter_search_allele_frequency_issues(xarray_dataset):
def test_xarray_get_allele_frequency_counts(xarray_datase

todo




def xarray_filter_homozygous(xr_df: xr.Dataset):

def xarray_filter_impact(xr_df: xr.Dataset, impact: list = ["HIGH", "MODERATE"]):

def xarray_filter_loftool_score(xr_df: xr.Dataset, loftool_score_ceiling: float = 0.5):

def xarray_filter_search_allele_frequency_issues(xr_df: xr.Dataset):

def xarray_get_allele_frequency_counts(xr_df: xr.Dataset):

def xarray_gene_multivariant_filter(xr_df: xr.Dataset):

def xarray_groupby_position(xr_df: xr.Dataset):


"""


import sgkit as sgkit
from sgkit.io.vcf import vcf_to_zarr

import xarray as xr
import zarr
import sys
import os
import argparse
import logging
import time
import numpy as np
import pandas as pd
import dask
import openpyxl
import datetime




# load is not working.  trying to add engine
def load_zarr_to_xarray(zarr_folder: str):
    """
    load a zarr file and convert to xarray
    perform a check to see if the xarray has the attribute column_order
    return the xarray
    zarr_folder: path to the zarr folder
    """
    # loading the zarr file as an xarray
    try:
        xr_df = xr.load_dataset(zarr_folder, engine="zarr")
    except:
        raise ValueError("The zarr file is not valid")

     # perform check to see if the xarray has the attribute column_order
    if "column_order" not in xr_df.attrs:
        raise ValueError("The xarray does not have the attribute column_order")

    return xr_df

def open_zarr_to_xarray(zarr_folder: str, drop_variables=[]):
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

    return xr_df

def xarray_filter_var_type(xr_df: xr.Dataset, filter_type: str = "exon"):
    """
    filter the xarray dataset for var_type
    default is exon for protein coding.  noncoding is for non-protein coding
    xr_df: xarray dataset
    """
    # raise an error if filter_type is not exon or noncoding
    if filter_type not in ["exon", "noncoding"]:
        raise ValueError("filter_type must be exon or noncoding")


    if filter_type == "noncoding":
        mask = (xr_df["var_type"] == "noncoding")
    elif filter_type == "exon":
        mask = (xr_df["var_type"] == "exon")
    else:
        raise ValueError("error in var_type where it is not exon or noncoding")

    return xr_df.where(mask, drop=True)





# TODO fix this function
def xarray_filter_max_af(xr_df: xr.Dataset, max_af_ceiling: float = 0.01):
    """
    filter the xarray dataset for max_af.  Keeps variants without a value for max_af
    xr_df: xarray dataset
    max_af: maximum allele frequency
    """
    # mask if max_af is less than the max_af_ceiling or if max_af is nan
    mask_max_af = (xr_df["max_af"] < max_af_ceiling) | (xr_df["max_af"].isnull())



    # filtering and creating files this way is very slow compared to setting it up in the varlociraptor config file
    return xr_df.where(mask_max_af, drop=True)







# impact of the variant
def xarray_filter_impact(xr_df: xr.Dataset, impact: list = ["HIGH", "MODERATE"]):
    """
    filter the xarray dataset for impact
    xr_df: xarray dataset
    impact: impact of the variant.  Default is ["HIGH", "MODERATE"].  Other options are "LOW", "MODIFIER"
    """
    # create filter for all rows where "impact" is in the list impact
    mask_impact = (xr_df["impact"].isin(impact))
    # filtering and creating files this way is very slow compared to setting it up in the varlociraptor config file
    return xr_df.where(mask_impact, drop=True)

# filter by loftool score
def xarray_filter_loftool_score(xr_df: xr.Dataset, loftool_score_ceiling: float = 0.5):
    """
    filter the xarray dataset for loftool score.  loftool stands for loss of function tool.  A score
    xr_df: xarray dataset
    loftool_score_ceiling: loftool score ceiling.  Default is 0.5.
    """
    # create filter for all rows where "loftool score" is less than the loftool score
    mask_loftool_score = (xr_df["loftool score"] < loftool_score_ceiling)
    # filtering and creating files this way is very slow compared to setting it up in the varlociraptor config file
    return xr_df.where(mask_loftool_score, drop=True)







# TODO:  determine if this is the best location to use flatten.
def xarray_to_csv(xr_df: xr.Dataset, flatten: bool = False,  file_name: str = "data_output.csv"):
    """
    convert xarray dataset to csv file


    xr_df: xarray dataset
    flatten: boolean value to determine if the xarray should be flattened.  Flattening with a single variant per row  Default is False
    file_name: name of the output file
    Any additional columns that are not in the column_order attribute will be added to the end of the csv file
    """
    # check to see if the xarray has the attribute column_order
    if "column_order" not in xr_df.attrs:
        raise ValueError("The xarray does not have the attribute column_order")

    # get the data variables from the xarray
    data_variables = xr_df.data_vars


    # maintain the order of the values in the column_order attribute but remove any values that have been removed and add any new values
    # determine the columns that have been removed
    removed_columns = set(xr_df.attrs["column_order"]) - set(data_variables)
    if len (removed_columns) > 0:
        xr_df.attrs["column_order"] = xr_df.attrs["column_order"] - list(removed_columns)


    # determine the columns that have been added
    gained_columns = set(data_variables) - set(xr_df.attrs["column_order"])
    # add any differences to the column_order attribute
    if len(gained_columns ) > 0:
        xr_df.attrs["column_order"] = xr_df.attrs["column_order"] + list(gained_columns)



    # check to see if file_name ends with .csv
    assert file_name.endswith('.csv'), "file_name must end with .csv"
    # turn xr into pandas and reorder the columns based on the column_order attribute in the xr
    filtered_pd = xr_df.to_pandas().reindex(columns=xr_df.attrs["column_order"])


    #todo add the xarray_groupby_var function
    # run xarray_groupby_var to group the data by position and chromosome
    #if flatten:
        # set the index to position and chromosome

        # flatten the data with a single variant per row.  This will create

    # save the file
    try:
        filtered_pd.to_csv(file_name)
    except:
        raise ValueError("Error saving file")


    return file_name


def argument_parser():
    """
    parse the arguments
    args: input file name, output file name
    args.input_type: type of input file, zarr, tsv, or csv
    """
    parser = argparse.ArgumentParser(description='transform or filter a file')
    parser.add_argument('--input', type=str, help='zarr folder name or names')
    parser.add_argument('--output', type=str, help='Output file name')
    args = parser.parse_args()

    # determine if the file ends with .zarr, tsv, or csv add the information to the args
    if args.input.endswith('.tsv'):
        args.input_type = 'tsv'
    elif args.input.endswith('.zarr'):
        args.input_type = 'zarr'
    elif args.input.endswith('.csv'):
        args.input_type = 'csv'
    else:
        raise ValueError("Input file must end with .tsv, .zarr, or .csv")

    return args





def logger_setup():
    """
    set up the logger
    logger: the logger

    """
    # set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logger = logging.getLogger(__name__)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    # create a file handler
    handler = logging.FileHandler('log_data_transformation.txt')
    handler.setLevel(logging.INFO)
    # create a logging format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(handler)

    return logger

#TODO change log file values in function
def save_log_file(log_file, log):
    """
    save the log file
    log_file: name of the log file
    log: the log
    """
    try:
        log_file = open(zarr_log_file, 'w')
        log_file.write(log)
        log_file.close()
    except:
        raise ValueError("Error saving log file")


def exon_filter(xr_df: xr.Dataset):
    """
    test the filter functions


    return the filtered xarray dataset
    """
    # filter the xarray for max_af
    filtered_xr_df = xarray_filter_max_af(xr_df, max_af_ceiling=0.01)
    # filter the xarray for protein
    filtered_xr_df = xarray_filter_var_type(filtered_xr_df, filter_type="exon")

    return filtered_xr_df


def noncoding_impact_filter(xr_df: xr.Dataset):
    """
    filter for noncoding and max_af


    return the filtered xarray dataset
    """
    # filter the xarray for max_af
    filtered_xr_df = xarray_filter_max_af(xr_df, max_af_ceiling=0.01)
    # filter the xarray for protein
    filtered_xr_df = xarray_filter_var_type(filtered_xr_df, filter_type="noncoding")
    #filtered_xr_df = xarray_filter_impact(filtered_xr_df, impact=["HIGH", "MODERATE"])

    return filtered_xr_df

def drop_columns(xr_df: xr.Dataset):
    """
    drop columns that appear to be useless
    """
    # get a list of the columns
    columns = list(xr_df.data_vars.keys())



    # drop specific columns to reduce the size of the dataset
    drop_columns = ["af",
    "afr_af",
    "eas_af",
    "amr_af",
    "appris",
    "biotype",
    "canonical",
    "ccds",
    "symbol_source",
    "trembl",
    "tsl",
    "pubmed",
    "sas_af",
    "max_af_pops",
    "eur_af",
    "distance",
    "end position",
    "ensp",
    "event",
    "flags",
    "hgnc_id",
    "hgvs_offset",
    "motif_name",
    "motif_pos",
    "motif_score_change",]

    # drop columns that are not needed to save space
    for drop in drop_columns:
        if drop in columns:
            xr_df = xr_df.drop_vars(drop)

    return xr_df

def drop_long_observation_columns(xr_df: xr.Dataset):
    """
    The long observation columns contain complete information about the variant.
    Normal processing does not require this information and it takes up a lot of space.
    This should be run as the first step in the process

    remove all columns that contain ": observations"

    """

    # get a list of all the columns
    columns = list(xr_df.data_vars.keys())

    # get a list of all the columns that contain the string "observations" but not "short"
    long_observation_columns = [column for column in columns if ": observations" in column and "short" not in column]

    # drop the columns
    xr_df = xr_df.drop_vars(long_observation_columns)

    return xr_df



def unique_value_counts(xr_df: xr.Dataset):
    """
    count the number of unique values for other columns when "chr_pos_alt_allele" is used as the index
    """

    # get all unique values from "chr_pos_alt_allele"
    unique_sites = np.unique(xr_df["chr_pos_alt_allele"])
    # get a count of the number of unique sites
    unique_sites_count = len(unique_sites)

    return unique_sites_count


def groupby_genome_location(xr_df: xr.Dataset):
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
        "var_type",
        ]


    unique_value_columns = [
                            ": allele frequency",
                            ": read depth",
                            "prob:"
                            "observations",]


    # add all values in unique_value_columns to the groupby list
    for column in columns:
        if column in additional:
            groupby_list.append(column)
        if any([unique in column for unique in unique_value_columns]):
            groupby_list.append(column)




    # get the unique value of the file name column
    file_name = xr_df["filename"].values[0]
    groupby_df = xr_df.to_pandas()
    # groupby the groupby_df using the groupby_list
    groupby_df = groupby_df.groupby(groupby_list, sort=False, dropna=False)
    groupby_df_agg = groupby_df.agg(['unique'])
    groupby_df_agg = groupby_df_agg.reset_index(drop=False)
    groupby_df_agg.columns = [''.join(col).strip() for col in groupby_df_agg.columns.values]

    return groupby_df_agg



def filter_data_from_csv(xr_df: xr.Dataset, file_name: str):
    """
    split data into files if lines in file_name match lines in xr_df
    """

    # read in file_name
    file_df = pd.read_csv(file_name)

    # get the column values from file_df
    file_df_columns = file_df.columns

    # determine which of the file_df columns are in the xr_df
    columns_in_xr_df = [column for column in file_df_columns if column in xr_df.data_vars]

    # determine which rows in each column of file_df match the xr_df
    #for column in columns_in_xr_df:








def main():

    # parse the arguments
    args = argument_parser()

    # set up the logger
    logger = logger_setup()


    # setup the output file name
    if args.output:
        output_file = args.output
    else:
        output_file = "output"

    if not output_file.endswith('.tsv'):
        output_file = output_file + '.tsv'


    # parse the input with space as the delimiter
    input_files = args.input.split(' ')

    # check that the input_files all end with .zarr
    for input_file in input_files:
        if not input_file.endswith('.zarr'):
            logger.error('Input file must be a zarr folder')
            exit("error: Input file must be a zarr folder")
        else:
            logger.info('confirmed zarr files')
            logger.info(f"input files: {input_files}\noutput file: {output_file}")


    print("completed input.  now load the zarr folder")


    # open the zarr file
    print("the time is now: ", datetime.datetime.now())
    xr_df = open_zarr_to_xarray(input_files[0])
    print("the time is now: ", datetime.datetime.now())
    print("completed loading zarr file")

    # drop columns that are not needed to save space
    xr_df = drop_columns(xr_df)

    print("dropping the long observation column. The time is now: ", datetime.datetime.now())
    xr_df = drop_long_observation_columns(xr_df)
    print("complete dropping long observtion column. The time is now: ", datetime.datetime.now())



    # print the number of rows in the xr_df before the filter
    print("xr_df size before xarray_filter_impact filter: ", xr_df.info)
    print("running impact filter.  the time is now: ", datetime.datetime.now())
    filtered_xr_df = xarray_filter_impact(xr_df, impact=["HIGH", "MODERATE"])
    # get the number of lines in the filtered_xr_df
    print("filtered_xr_df size: ", filtered_xr_df.info)
    print("running max_af filter.  the time is now: ", datetime.datetime.now())
    filtered_xr_df = xarray_filter_max_af(filtered_xr_df , max_af_ceiling=0.2)
    print("complete max_af filter.  the time is now: ", datetime.datetime.now())
    print("filtered_xr_df size: ", filtered_xr_df.info)


    # save the filtered_xr_df to a tsv file called filter_test_deleteme.tsv
    #print("saving filtered_xr_df to a tsv file.  the time is now: ", datetime.datetime.now())
    #filtered_xr_df.to_csv("filter_test_deleteme.tsv", sep="\t")


    #exon_file_name = "exon_filter_" + output_file
    #xarray_to_csv(exon_filtered_xr_df, file_name=exon_file_name)
    #print(f"impact filter file saved as {exon_file_name}")
    #logger.info(f"impact filter file saved as {exon_file_name}")


    # print the number of rows in the filtered_xr_df
    print("filtered_xr_df size before groupby: ", filtered_xr_df.info)
    # save the filtered_xr_df to a tsv file

    print("started groupby_genome_location function ", datetime.datetime.now())
    filtered_xr_df = groupby_genome_location(filtered_xr_df)
     # print the number of rows in the filtered_xr_df
    print("filtered_xr_df size after groupby: ", filtered_xr_df.info)

    # filtered_xr_df = groupby_genome_location(filtered_xr_df)
    file_name = filtered_xr_df["filename"].values[0]
    print("filtered_xr_df size after groupby: ", filtered_xr_df.info)
    filtered_xr_df.to_csv(f"genome_location_groupby_{file_name}", sep="\t")
    print("completed groupby_genome_location function ", datetime.datetime.now())
    # print all rows the the value  135762167 in the column "position"

    # print the number of rows in the filtered_xr_df
    print("filtered_xr_df size after saving: ", filtered_xr_df.info)
    """




    # noncoding_filter
    noncoding_impactHM_filtered_xr_df = noncoding_impact_filter(xr_df)

    # noncoding save to file
    noncoding_impactHM_file_name = "noncoding_impact_filter_" + output_file
    xarray_to_csv(noncoding_impactHM_filtered_xr_df, file_name=noncoding_impactHM_file_name)
    print(f"impact filter file saved as {noncoding_impactHM_file_name}")
    logger.info(f"impact filter file saved as {noncoding_impactHM_file_name}")

    """

    # save the number of rows to the log file
    logger.info(f"number of rows in filtered_xr_df at end: {filtered_xr_df.info}")


    # close the logging file
    logger.info('Completed zarr transformations and closing log file')
    # close the log file


if __name__ == "__main__":
    main()
