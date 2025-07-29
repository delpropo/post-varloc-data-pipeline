#!/usr/bin/env python3
import sys
from pathlib import Path

proj_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(proj_root))

from post_varloc_data_pipeline.config import get_yaml_value, parse_yaml

# Test YAML config
yaml_path = proj_root / "config.yaml"

print(f"YAML path exists: {yaml_path.exists()}")

if yaml_path.exists():
    config_format = "yaml"
else:
    config_format = "unknown"

print(f"Config format: {config_format}")

# Test the actual function call
if config_format == "yaml":
    result = get_yaml_value(['ADDITIONAL_ZARR_FILTERING', 'OUTPUT_DIR'])
    print(f"Result from get_yaml_value: {result}")

    data = parse_yaml()
    result2 = data.get('ADDITIONAL_ZARR_FILTERING', {}).get('OUTPUT_DIR')
    print(f"Result from direct parse: {result2}")
else:
    print("No YAML config found")
