from pathlib import Path

from dotenv import load_dotenv, find_dotenv
from loguru import logger
import os

import configparser
import yaml

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



def parse_ini(ini_path=None):
    """
    Parse the config.ini file and return a ConfigParser object.
    If ini_path is None, defaults to PROJ_ROOT/config.ini.
    """
    if ini_path is None:
        ini_path = PROJ_ROOT / "config.ini"
    config = configparser.ConfigParser()
    config.read(str(ini_path))
    return config

def parse_yaml(yaml_path=None):
    """
    Parse a YAML file and return the loaded dictionary.
    If yaml_path is None, defaults to PROJ_ROOT/config.yaml.
    """
    if yaml_path is None:
        yaml_path = PROJ_ROOT / "config.yaml"
    with open(yaml_path, 'r') as f:
        return yaml.safe_load(f)

def parse_config(config_path=None):
    """
    Parse a config file (INI or YAML) and return the appropriate object.
    If config_path is None, tries config.ini then config.yaml in PROJ_ROOT.
    """
    if config_path is None:
        ini_path = PROJ_ROOT / "config.ini"
        yaml_path = PROJ_ROOT / "config.yaml"
        if ini_path.exists():
            config_path = ini_path
        elif yaml_path.exists():
            config_path = yaml_path
        else:
            raise FileNotFoundError("No config.ini or config.yaml found in project root.")
    config_path = Path(config_path)
    if config_path.suffix in ['.yaml', '.yml']:
        return parse_yaml(config_path)
    else:
        return parse_ini(config_path)


def get_ini_value(section, key, ini_path=None, fallback=None):
    """
    Get a value from the config.ini file by section and key.
    Returns fallback if not found.
    """
    config = parse_ini(ini_path)
    return config.get(section, key, fallback=fallback)

def get_yaml_value(key_path, yaml_path=None, fallback=None):
    """
    Get a value from a YAML config file using a list of keys (key_path).
    Example: key_path=["section", "subkey"]
    Returns fallback if not found.
    """
    data = parse_yaml(yaml_path)
    d = data
    try:
        for k in key_path:
            d = d[k]
        return d
    except (KeyError, TypeError):
        return fallback

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


