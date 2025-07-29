#!/usr/bin/env python3
"""
Simple script to extract configuration values from YAML config files for Makefiles.
Only supports YAML format (config.yaml).

Usage:
    python scripts/get_config_value.py --section SECTION --key KEY [--fallback VALUE]
    python scripts/get_config_value.py --section SECTION --all-keys
    python scripts/get_config_value.py --section SECTION --check-required KEY1 KEY2 ...

Examples:
    python scripts/get_config_value.py --section SLURM --key slurm_account
    python scripts/get_config_value.py --section slurm_zarr_groupby_aggregator --key cpus-per-task
    python scripts/get_config_value.py --section ADDITIONAL_ZARR_FILTERING --all-keys
"""

import argparse
import sys
from pathlib import Path

# Add the project root to the path so we can import config
proj_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(proj_root))

# Import after path modification
from post_varloc_data_pipeline.config import get_yaml_value, parse_yaml


def get_config_value_yaml(section, key=None, fallback=None):
    """Get value from YAML config."""
    if key:
        return get_yaml_value([section, key], fallback=fallback)
    else:
        data = parse_yaml()
        return data.get(section, {})


def main():
    parser = argparse.ArgumentParser(
        description="Extract configuration values from config.yaml for Makefiles"
    )
    parser.add_argument(
        "--section",
        required=True,
        help="Configuration section name"
    )
    parser.add_argument(
        "--key",
        help="Configuration key name (required unless --all-keys is used)"
    )
    parser.add_argument(
        "--fallback",
        help="Fallback value if key is not found"
    )
    parser.add_argument(
        "--all-keys",
        action="store_true",
        help="Print all key=value pairs in the section"
    )
    parser.add_argument(
        "--check-required",
        nargs="+",
        help="Check that all specified keys exist in the section (exit 1 if any missing)"
    )
    parser.add_argument(
        "--get-flags",
        action="store_true",
        help="Convert boolean config values to command-line flags"
    )

    args = parser.parse_args()

    try:
        if args.all_keys:
            # Print all keys in the section
            section_data = get_config_value_yaml(args.section)
            if not section_data:
                print(f"Error: Section [{args.section}] not found in config.yaml", file=sys.stderr)
                sys.exit(1)

            for key, value in section_data.items():
                print(f"{key}={value}")

        elif args.get_flags:
            # Convert boolean config values to command-line flags
            section_data = get_config_value_yaml(args.section)
            if not section_data:
                print(f"Error: Section [{args.section}] not found in config.yaml", file=sys.stderr)
                sys.exit(1)

            flags = []
            # SLURM parameters to exclude from application flags
            slurm_params = {'cpus-per-task', 'mem-per-cpu', 'mem', 'time', 'txt_file'}

            for key, value in section_data.items():
                # Skip SLURM-specific parameters
                if key in slurm_params:
                    continue

                # Handle boolean flags
                if isinstance(value, bool) and value:
                    flag_name = key.replace('_', '-')
                    flags.append(f"--{flag_name}")
                # Handle numeric parameters
                elif isinstance(value, (int, float)) and not isinstance(value, bool):
                    flag_name = key.replace('_', '-')
                    flags.append(f"--{flag_name} {value}")
                # Handle string parameters
                elif isinstance(value, str):
                    flag_name = key.replace('_', '-')
                    flags.append(f"--{flag_name} {value}")

            print(' '.join(flags))

        elif args.check_required:
            # Check that all required keys exist
            section_data = get_config_value_yaml(args.section)
            if not section_data:
                print(f"Error: Section [{args.section}] not found in config.yaml", file=sys.stderr)
                sys.exit(1)

            missing_keys = []
            for key in args.check_required:
                if key not in section_data or not str(section_data[key]).strip():
                    missing_keys.append(key)

            if missing_keys:
                print(f"Error: Missing required keys in [{args.section}]: {', '.join(missing_keys)}", file=sys.stderr)
                sys.exit(1)
            else:
                print("All required keys found")

        else:
            # Get a single key value
            if not args.key:
                print("Error: --key is required unless --all-keys or --check-required is used", file=sys.stderr)
                sys.exit(1)

            value = get_config_value_yaml(args.section, args.key, args.fallback)
            if value is None:
                print(f"Error: Key '{args.key}' not found in section [{args.section}]", file=sys.stderr)
                sys.exit(1)

            print(value)

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
