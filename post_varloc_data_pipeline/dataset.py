from pathlib import Path

import typer
from loguru import logger
from tqdm import tqdm
# from post_varloc_data_pipeline.config import PROCESSED_DATA_DIR, RAW_DATA_DIR
import os

# print the current working directory
print(os.getcwd())
RAW_DATA_DIR = Path("data/raw")
PROCESSED_DATA_DIR = Path("data/processed")

# print the full path of the RAW_DATA_DIR
print(RAW_DATA_DIR.resolve())
# print the full path of the PROCESSED_DATA_DIR
print(PROCESSED_DATA_DIR.resolve())





app = typer.Typer()


@app.command()
def main(
    # ---- REPLACE DEFAULT PATHS AS APPROPRIATE ----
    input_path: Path = RAW_DATA_DIR / "dataset.csv",
    output_path: Path = PROCESSED_DATA_DIR / "dataset.csv",
    source: Path = typer.Option(..., help="The folder path to use")

    # ----------------------------------------------
):
    # ---- REPLACE THIS WITH YOUR OWN CODE ----
    logger.info("Processing dataset")
    # print the input path
    print(f"Input path: {input_path}")
    # print the output path
    print(f"Output path: {output_path}")

    # if source ends in .tsv then create a symlink to the RAW_DATA_DIR location
    if source.suffix == '.tsv':
        print(f"Creating symbolic link: {RAW_DATA_DIR} -> {source}")
        create_tsv_symlink(source, RAW_DATA_DIR)
    else:
        source_data_tables = source / "results" / "tables"
        create_symlink_folder(source_data_tables, RAW_DATA_DIR)

    for i in tqdm(range(10), total=10):
        if i == 5:
            logger.info("Something happened for iteration 5.")
    logger.success("Processing dataset complete.")
    # -----------------------------------------
    # print the source path

def create_tsv_symlink(tsv_path, RAW_DATA_DIR):
    """
    Create a symbolic link for a .tsv file in the RAW_DATA_DIR.
    This function checks if the source path ends with '.tsv' and exists. If both conditions are met,
    it creates a symbolic link to the RAW_DATA_DIR location.
    Args:
        source (str): The source file path that should end with '.tsv'.
        RAW_DATA_DIR (Path): The directory where the symbolic link will be created.
    Returns:
        None
    Prints:
        A message indicating whether the source path does not end with '.tsv' or does not exist.
    """
    # confirm that source ends with .tsv and exists
    if not tsv_path.suffix == '.tsv':
        print(f"The source path '{source}' does not end with '.tsv'.")
        return
    # create a symlink to the RAW_DATA_DIR location
    try:
        # get the file name from the tsv_path and append it to the RAW_DATA_DIR path
        symlink_destination = RAW_DATA_DIR / tsv_path.name
        os.symlink(tsv_path, symlink_destination)
    except FileNotFoundError:
        print(f"The source path '{tsv_path}' does not exist.")
    except OSError as e:
        print(f"Failed to create symbolic link: {e}")


def create_symlink_folder(source, RAW_DATA_DIR):
    # confirm that the source and RAW_DATA_DIR are directories
    if not os.path.isdir(source):
        print(f"The source path '{source}' is not a directory.")
        return
    if not os.path.isdir(RAW_DATA_DIR):
        print(f"The destination path '{RAW_DATA_DIR}' is not a directory.")
        return
    # add /results/tables to source and create path raw_data_tables

    tsv_files = [f for f in os.listdir(source) if f.endswith('.tsv')]
    for tsv_file in tsv_files:
        try:
            create_tsv_symlink(source / tsv_file, RAW_DATA_DIR)
            print(f"Symbolic link created: {RAW_DATA_DIR} -> {source / tsv_file}")
        except FileNotFoundError:
            print(f"The source path '{source / tsv_file}' does not exist.")
        except OSError as e:
            print(f"Failed to create symbolic link: {e}")





if __name__ == "__main__":
    app()


    #source_path = input("Enter the source path: ")
    #link_name = input("Enter the symbolic link name: ")
    #create_symlink(source_path, RAW_DATA_DIR)
