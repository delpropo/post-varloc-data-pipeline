import tempfile
import shutil
from pathlib import Path
import configparser
import pytest

from post_varloc_data_pipeline import config as config_mod


def test_parse_ini_and_get_ini_value(tmp_path):
    # Create a temporary ini file
    ini_content = """
    [section1]
    key1 = value1
    key2 = value2

    [section2]
    keyA = valueA
    """
    ini_file = tmp_path / "test_config.ini"
    ini_file.write_text(ini_content)

    # Test parse_ini
    cfg = config_mod.parse_ini(ini_file)
    assert isinstance(cfg, configparser.ConfigParser)
    assert cfg.get("section1", "key1") == "value1"
    assert cfg.get("section1", "key2") == "value2"
    assert cfg.get("section2", "keyA") == "valueA"

    # Test get_ini_value
    assert config_mod.get_ini_value("section1", "key1", ini_path=ini_file) == "value1"
    assert config_mod.get_ini_value("section2", "keyA", ini_path=ini_file) == "valueA"
    # Test fallback
    assert config_mod.get_ini_value("section1", "nonexistent", ini_path=ini_file, fallback="default") == "default"
    # Test missing section returns fallback
    assert config_mod.get_ini_value("nope", "nope", ini_path=ini_file, fallback="fallback") == "fallback"


def test_parse_config_and_get_value_from_files():
    """
    Test parse_config and value retrieval for both INI and YAML files in tests/config.
    """
    import os
    test_dir = os.path.dirname(__file__)
    config_dir = os.path.join(test_dir, "config")
    ini_path = os.path.join(config_dir, "test_config.ini")
    yaml_path = os.path.join(config_dir, "test_config.yaml")

    # INI file tests
    ini_cfg = config_mod.parse_ini(ini_path)
    assert ini_cfg.get("slurm_create_tsv", "cpus-per-task") == "1"
    assert ini_cfg.get("slurm_zarr_groupby_aggregator", "txt_file") == "sooeunc_aggregator.txt"
    # get_ini_value
    assert config_mod.get_ini_value("slurm_create_tsv", "mem", ini_path=ini_path) == "7G"
    assert config_mod.get_ini_value("SAMPLE_INFO", "family", ini_path=ini_path) == "false"

    # YAML file tests
    yaml_cfg = config_mod.parse_yaml(yaml_path)
    # Check some known keys (example: FILTERS -> ANN['MAX_AF'])
    assert "FILTERS" in yaml_cfg
    assert "ANN['MAX_AF']" in yaml_cfg["FILTERS"]
    assert yaml_cfg["FILTERS"]["ANN['MAX_AF']"] == "<=:0.2"
    # get_yaml_value
    assert config_mod.get_yaml_value(["FILTERS", "ANN['MAX_AF']"], yaml_path=yaml_path) == "<=:0.2"
    # Test fallback for missing key
    assert config_mod.get_yaml_value(["NOT_A_SECTION", "nope"], yaml_path=yaml_path, fallback="fallback") == "fallback"
