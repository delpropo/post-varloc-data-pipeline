from pathlib import Path

from dotenv import load_dotenv, find_dotenv
from loguru import logger
import os

# Load environment variables from .env file if it exists
load_dotenv(find_dotenv())

# Paths
PROJ_ROOT = Path(__file__).resolve().parents[1]
logger.info(f"PROJ_ROOT path is: {PROJ_ROOT}")

DATA_DIR = PROJ_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
INTERIM_DATA_DIR = DATA_DIR / "interim"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
EXTERNAL_DATA_DIR = DATA_DIR / "external"

MODELS_DIR = PROJ_ROOT / "models"

REPORTS_DIR = PROJ_ROOT / "reports"
FIGURES_DIR = REPORTS_DIR / "figures"

# add test directory and test data locations
TEST_DIR = PROJ_ROOT / "tests"
TEST_DATA_DIR = TEST_DIR / "data"

# If tqdm is installed, configure loguru with tqdm.write
# https://github.com/Delgan/loguru/issues/135
try:
    from tqdm import tqdm

    logger.remove(0)
    logger.add(lambda msg: tqdm.write(msg, end=""), colorize=True)
except ModuleNotFoundError:
    pass

# print all values in .env file
logger.info("Environment variables:")
for key in os.environ:
    logger.info(f"{key}: {os.environ[key]}")

    # check to see if SNAKEMAKE_ANALYSIS_TABLE_RESULTS contains .tsv files
    if key == "SNAKEMAKE_ANALYSIS_TABLE_RESULTS":
        tsv_files = [f for f in os.listdir(os.environ[key]) if f.endswith('.tsv')]
        if tsv_files:
            logger.info(f"List of .tsv files: {tsv_files}")
            # make the SNAKEMAKE_ANALYSIS_TABLE_RESULTS_ZARR directory if it doesn't exist
            if not os.path.exists(os.environ["SNAKEMAKE_ANALYSIS_TABLE_RESULTS_ZARR"]):
                os.makedirs(os.environ["SNAKEMAKE_ANALYSIS_TABLE_RESULTS_ZARR"])
        else:
            logger.info(f"No .tsv files found in {key}, using ")
            # print all files in the SNKEMAKE_ANALYSIS_TABLE_RESULTS directory
            print(f"List of files in {key}: {os.listdir(os.environ[key])}")
            print(f"List of .tsv files: {tsv_files}")
