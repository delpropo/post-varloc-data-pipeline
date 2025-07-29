#!/usr/bin/env python3
"""
Debug version to understand what's happening
"""

import argparse
import sys
from pathlib import Path

# Add the project root to the path so we can import config
proj_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(proj_root))

# Import after path modification
from post_varloc_data_pipeline.config import get_yaml_value, parse_yaml

def test_config():
    print(f"Project root: {proj_root}")

    yaml_path = proj_root / "config.yaml"

    print(f"YAML exists: {yaml_path.exists()}")

    # Test YAML directly
    if yaml_path.exists():
        print("\nTesting YAML directly:")
        try:
            result = get_yaml_value(['ADDITIONAL_ZARR_FILTERING', 'OUTPUT_DIR'])
            print(f"get_yaml_value result: {result}")
        except Exception as e:
            print(f"get_yaml_value error: {e}")

        try:
            data = parse_yaml()
            section = data.get('ADDITIONAL_ZARR_FILTERING', {})
            print(f"Section keys: {list(section.keys())}")
            result = section.get('OUTPUT_DIR')
            print(f"Direct access result: {result}")
        except Exception as e:
            print(f"Direct access error: {e}")

if __name__ == "__main__":
    test_config()
