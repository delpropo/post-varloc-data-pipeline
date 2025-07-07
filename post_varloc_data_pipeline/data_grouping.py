import os
import pandas as pd
import logging
import argparse
import time

def argument_parser() -> argparse.Namespace:
    """
    Parse the arguments for input and output folders.
    """
    parser = argparse.ArgumentParser(description='Process TSV files in a folder and group data.')
    parser.add_argument('--input', type=str, required=True, help='Input folder containing TSV files')
    parser.add_argument('--output', type=str, help='Output folder location', default=None)
    args = parser.parse_args()

    if not os.path.isdir(args.input):
        raise ValueError("Input path must be a directory")

    if args.output is None:
        args.output = os.path.join(args.input, 'grouped')
        if not os.path.exists(args.output):
            os.makedirs(args.output)
    else:
        if not os.path.isdir(args.output):
            raise ValueError("Output path must be a directory")

    return args

def counting_variant_occurrences(df: pd.DataFrame) -> pd.DataFrame:
    """
    Count the number of families that have the same variant alternative allele at the same position
    and the count of unique values for variant location for specific alleles.

    :param df: Input pandas dataframe
    :return: Dataframe with additional columns for variant occurrences and unique alternative allele count
    """
    # Remove existing columns if they exist
    df.drop(columns=[col for col in df.columns if col in ['variant_occurance_between_families_count', 'unique_alternative_allele_count', 'location_occurance_across_families']], inplace=True, errors='ignore')
    # make all chromosome values type string
    df['chromosome'] = df['chromosome'].astype(str)
    # make all position values type int
    df['position'] = df['position'].astype(int)
    # Pivot on chromosome and position and get the total number of unique families
    df_pivot = df.pivot_table(index=['chromosome', 'position', 'alternative allele'], values='family', aggfunc='count').reset_index()
    df_pivot.rename(columns={'family': 'variant_occurance_between_families_count'}, inplace=True)
    df = df.merge(df_pivot, on=['chromosome', 'position', 'alternative allele'], how='left')

    # Get a count of the unique alleles for each chromosome and position
    df_pivot = df.pivot_table(index=['chromosome', 'position'], values='alternative allele', aggfunc=lambda x: x.nunique()).reset_index()
    df_pivot.rename(columns={'alternative allele': 'unique_alternative_allele_count'}, inplace=True)
    df = df.merge(df_pivot, on=['chromosome', 'position'], how='left')

    # Create a column that has the number of times a variant occurs at a specific location across all families
    df['location_occurance_across_families'] = df.groupby(['chromosome', 'position'])['position'].transform('count')

    # Reorder columns
    starting_columns = ['chromosome', 'position', 'allele', 'reference allele', 'hgvsg', 'protein alteration (short)' 'family', 'symbol', 'variant_class', 'impact', 'max_af', 'gnomadg_af', 'variant_occurance_between_families_count', 'unique_alternative_allele_count', 'location_occurance_across_families']
    read_depth_prob_filename_columns = [col for col in df.columns if any(x in col for x in ["read depth", "prob:", "filename"])]
    allele_frequency_columns = [col for col in df.columns if ": allele frequency" in col]
    observations_columns = [col for col in df.columns if "observations" in col]
    end_columns = [col for col in df.columns if col not in starting_columns + read_depth_prob_filename_columns + allele_frequency_columns + observations_columns]
    # combine the columns into a single list
    combined_columns = starting_columns + allele_frequency_columns + observations_columns + read_depth_prob_filename_columns + end_columns
    # remove any duplicates or columns that are not in the dataframe
    combined_columns = list(dict.fromkeys([col for col in combined_columns if col in df.columns]))
    df = df[combined_columns]

    return df

def get_tsv_files(folder_path):
    tsv_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.tsv')]
    # make all paths absolute
    tsv_files = [os.path.abspath(f) for f in tsv_files]
    return tsv_files

def logger_setup(args, log_file_prefix: str = 'data_grouping') -> logging.Logger:
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


# create an error check function.


def main():
    args = argument_parser()
    logger = logger_setup(args)
    logger.info(f"Folder path: {args.input}")
    logger.info(f"Files in folder: {os.listdir(args.input)}")

    tsv_files = get_tsv_files(args.input)
    logger.info(f"TSV files: {tsv_files}")
    dataframes = []
    for file in tsv_files:
        try:
            logger.info(f"Reading file: {file}")
            df = pd.read_csv(file, sep='\t', header=0, low_memory=False)
            dataframes.append(df)
        except pd.errors.ParserError as e:
            logger.error(f"Error reading {file}: {e}")
            continue  # Skip the problematic file

    if len(dataframes) == 0:
        logger.info("No valid TSV files found in the folder")
        return

    combined_df = pd.concat(dataframes, ignore_index=True)

    # Create 'grouped' subfolder if it doesn't exist
    grouped_folder = os.path.join(args.input, 'grouped')
    if not os.path.exists(grouped_folder):
        os.makedirs(grouped_folder)

    # run counting_variant_occurrences
    combined_df = counting_variant_occurrences(combined_df)

    row_count = combined_df.shape[0]
    unique_family_count = combined_df['family'].nunique()
    for i in range(1, unique_family_count + 1):
        # print out the number of rows which have 'variant_occurance_between_families_count' equal to i
        logger.info(f"Number of rows with 'variant_occurance_between_families_count' equal to {i}: {combined_df[combined_df['variant_occurance_between_families_count'] == i].shape[0]}")

    removal_number = 4
    # remove all rows where 'variant_occurance_between_families_count' is greater than removal_number
    combined_df = combined_df[combined_df['variant_occurance_between_families_count'] <= removal_number]
    # print out the number of rows removed
    logger.info(f"Removed {row_count - combined_df.shape[0]} rows with 'variant_occurance_between_families_count' greater than {removal_number}")


    # remove all rows where 'location_occurance_across_families' is greater than removal_number
    row_count = combined_df.shape[0]
    combined_df = combined_df[combined_df['location_occurance_across_families'] <= removal_number]
    # print out the number of rows removed
    logger.info(f"Removed {row_count - combined_df.shape[0]} rows with 'location_occurance_across_families' greater than {removal_number}")

    # resort by chromosome and position
    combined_df = combined_df.sort_values(by=['chromosome', 'position'])

    sorted_family_string = '_'.join(sorted(combined_df['family'].unique()))
    # create a save file by getting all the unique values of the 'family' column, sorting them, and joining them with an underscore
    save_file = sorted_family_string + '_grouped_data.tsv'
    output_file = os.path.join(grouped_folder, save_file)
    combined_df.to_csv(output_file, sep='\t', index=False)
    logger.info(f"Combined data saved to {output_file}")




    # print out a count of the number of rows in the dataframe for each family for each chromosome
    for family in combined_df['family'].unique():
        logger.info(f"Family: {family}")
        for chromosome in combined_df['chromosome'].unique():
            logger.info(f"Row count Chromosome {chromosome}: {combined_df[(combined_df['family'] == family) & (combined_df['chromosome'] == chromosome)].shape[0]}")

if __name__ == "__main__":
    main()