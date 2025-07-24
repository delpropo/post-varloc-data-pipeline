#!/usr/bin/env python3
"""
Create filtered and pivoted Zarr files based on configuration criteria defined in config.ini.
Supports column filtering, data filtering, gene filtering, and pivot operations for genomic variant analysis.

WORKFLOW:
1. Apply gene filtering (if specified) to keep only variants with genes in the filter list
2. Filter data based on configuration criteria (config.ini)
3. Apply pivot operations to convert long format to wide format
4. Aggregate columns with identical/different values appropriately
5. Add filename metadata and save processed zarr file

GENE FILTERING:
- Accepts TSV file with 'Gene Symbol' column
- Filters variants where any gene in ANN['SYMBOL'] matches any gene in the filter list
- ANN['SYMBOL'] can contain multiple genes separated by semicolons, commas, pipes, or ampersands
- Applied before other filtering steps to maximize performance
"""

import argparse
import configparser
import pandas as pd
import xarray as xr
from pathlib import Path
from typing import Dict, List, Any
import warnings

# Suppress Zarr warnings
warnings.filterwarnings('ignore', message='.*vlen-utf8.*')
warnings.filterwarnings('ignore', message='.*StringDType.*')
warnings.filterwarnings('ignore', message='.*Consolidated metadata.*')


class ZarrFilterPivotCreator:
    """Create filtered and pivoted Zarr files based on config.ini criteria."""

    def __init__(self, config_file: str = "config.ini"):
        """Initialize with configuration file."""
        self.config_file = config_file
        self.config = None
        # Essential columns for pivot operations
        self.essential_columns = ['SAMPLE', 'CHROM', 'POS', 'REF', 'ALT', "FORMAT['SAOBS']"]
        # Additional essential columns to include if they exist (removed ANN['MAX_AF'] to preserve null values)
        self.additional_essential = []
        self.load_config(config_file)
        # Gene filtering
        self.gene_filter_symbols = None

    def load_config(self, config_file: str) -> None:
        """Load configuration from INI file."""
        self.config = configparser.ConfigParser()
        self.config.read(config_file)

    def load_gene_filter(self, gene_filter_file: str) -> None:
        """Load gene symbols from TSV file for filtering."""
        try:
            # Read the gene filter file
            gene_df = pd.read_csv(gene_filter_file, sep='\t')

            # Check if Gene Symbol column exists
            if 'Gene Symbol' not in gene_df.columns:
                raise ValueError(f"Gene filter file must contain a 'Gene Symbol' column. Found columns: {list(gene_df.columns)}")

            # Extract unique gene symbols and remove any null values
            self.gene_filter_symbols = set(gene_df['Gene Symbol'].dropna().unique())

            print(f"✓ Loaded {len(self.gene_filter_symbols)} gene symbols for filtering")
            print(f"  Example genes: {list(self.gene_filter_symbols)[:5]}")

        except Exception as e:
            print(f"Error loading gene filter file '{gene_filter_file}': {e}")
            raise

    def apply_gene_filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply gene filtering if gene filter is loaded."""
        if self.gene_filter_symbols is None:
            return df

        if "ANN['SYMBOL']" not in df.columns:
            print("Warning: ANN['SYMBOL'] column not found in data. Skipping gene filtering.")
            return df

        original_rows = len(df)

        # Create a mask for rows where any gene symbol in ANN['SYMBOL'] matches our filter
        def check_gene_match(symbols_str):
            if pd.isna(symbols_str) or symbols_str == '' or symbols_str == '.':
                return False

            # ANN['SYMBOL'] can contain multiple symbols separated by various delimiters
            # Common delimiters: semicolon, comma, pipe, ampersand
            symbols = str(symbols_str).replace(';', '|').replace(',', '|').replace('&', '|').split('|')
            symbols = [s.strip() for s in symbols if s.strip()]

            # Check if any symbol matches our filter
            return any(symbol in self.gene_filter_symbols for symbol in symbols)

        # Apply the filter
        mask = df["ANN['SYMBOL']"].apply(check_gene_match)
        filtered_df = df[mask].copy()

        filtered_rows = len(filtered_df)
        removed_rows = original_rows - filtered_rows

        print("🧬 GENE FILTERING APPLIED:")
        print(f"   ORIGINAL: {original_rows:,} rows")
        print(f"   FILTERED: {filtered_rows:,} rows")
        print(f"   REMOVED:  {removed_rows:,} rows ({(removed_rows/original_rows)*100:.1f}%)")

        return filtered_df

    def get_columns_to_drop(self) -> List[str]:
        """Get list of columns to drop based on configuration."""
        drop_columns = []

        # Check COLUMN_MANAGEMENT section first
        if self.config.has_section('COLUMN_MANAGEMENT') and self.config.has_option('COLUMN_MANAGEMENT', 'drop_columns'):
            drop_columns.extend([col.strip() for col in self.config.get('COLUMN_MANAGEMENT', 'drop_columns').split(',')])

        # Also check legacy DROP_COLUMNS section for backward compatibility
        if self.config.has_section('DROP_COLUMNS') and self.config.has_option('DROP_COLUMNS', 'columns'):
            drop_columns.extend([col.strip() for col in self.config.get('DROP_COLUMNS', 'columns').split(',')])

        # Remove empty strings and duplicates
        drop_columns = list(set([col for col in drop_columns if col.strip()]))
        return drop_columns

    def get_filters(self) -> Dict[str, Dict[str, Any]]:
        """Get filtering criteria from configuration."""
        filters = {}
        if not self.config.has_section('FILTERS'):
            return filters

        for column, filter_str in self.config.items('FILTERS'):
            # Skip DEFAULT section items that get inherited
            if column in self.config.defaults():
                continue

            if ':' in filter_str:
                operator, value = filter_str.split(':', 1)

                # Convert value to appropriate type
                if value.replace('.', '').replace('-', '').isdigit():
                    if '.' in value:
                        value = float(value)
                    else:
                        value = int(value)
                elif value.lower() in ['true', 'false']:
                    value = value.lower() == 'true'

                # ConfigParser converts keys to lowercase by default
                # We need to find the actual column name with correct case
                original_column = column

                # Check if this is a case-sensitivity issue - try to match against actual columns in data
                # We'll handle this during filtering when we have access to the DataFrame columns
                filters[original_column] = {'operator': operator, 'value': value}

        return filters

    def apply_filter(self, df: pd.DataFrame, column: str, filter_def: Dict[str, Any]) -> pd.Series:
        """Apply a single filter condition to a DataFrame column."""
        operator = filter_def.get('operator', '==')
        value = filter_def.get('value')

        # Handle null/blank values - always keep them for frequency filters
        is_frequency_column = 'AF' in column.upper() or 'freq' in column.lower()

        # Create comprehensive null mask for frequency columns (including various representations of missing data)
        null_mask = (df[column].isnull() |
                    (df[column] == '') |
                    (df[column] == '.') |
                    (df[column] == 'NA') |
                    (df[column] == 'NULL') |
                    (df[column] == 'null'))

        # For frequency columns, convert to numeric, setting non-numeric values to NaN
        if is_frequency_column:
            # Convert to numeric, coercing errors to NaN
            numeric_column = pd.to_numeric(df[column], errors='coerce')
        else:
            numeric_column = df[column]

        if operator == '==':
            mask = numeric_column == value
        elif operator == '!=':
            mask = numeric_column != value
        elif operator == '<':
            mask = numeric_column < value
        elif operator == '<=':
            mask = numeric_column <= value
        elif operator == '>':
            mask = numeric_column > value
        elif operator == '>=':
            mask = numeric_column >= value
        elif operator == 'in':
            mask = numeric_column.isin(value if isinstance(value, list) else [value])
        elif operator == 'not_in':
            mask = ~numeric_column.isin(value if isinstance(value, list) else [value])
        elif operator == 'contains':
            mask = df[column].str.contains(str(value), na=False)
        elif operator == 'not_contains':
            mask = ~df[column].str.contains(str(value), na=False)
        else:
            raise ValueError(f"Unknown operator: {operator}")

        # For frequency columns, ALWAYS include rows where the value is null/blank
        # This ensures variants without frequency data (often novel variants) are preserved
        if is_frequency_column:
            null_count = null_mask.sum()
            if null_count > 0:
                print(f"  Including {null_count} rows with missing {column} values (preserving novel variants)")
                mask = mask | null_mask

            # Debug: Show how many rows match each condition
            valid_rows = (~df[column].isnull() & (df[column] != '') & (df[column] != '.') &
                         (df[column] != 'NA') & (df[column] != 'NULL') & (df[column] != 'null'))
            numeric_valid = pd.to_numeric(df[column], errors='coerce').notna()

            print(f"  Debug for {column}: Total rows={len(df)}, Null/blank={null_count}, "
                  f"Valid non-null={valid_rows.sum()}, Numeric valid={numeric_valid.sum()}")

            if operator in ['<=', '<', '>=', '>']:
                numeric_match = (~numeric_column.isnull()) & eval(f"numeric_column {operator} {value}")
                print(f"  Numeric condition matches: {numeric_match.sum()}")
                print(f"  Final mask (numeric + nulls): {mask.sum()}")

        return mask

    def create_filtered_pivoted_zarr(self, zarr_path: str, output_path: str = None, remove_validation_errors: bool = False) -> tuple[str, pd.DataFrame]:
        """Create a filtered and pivoted Zarr file based on configuration criteria."""
        print(f"Loading Zarr file: {zarr_path}")

        # Load the Zarr file
        ds = xr.open_zarr(zarr_path)
        df = ds.to_dataframe().reset_index()

        original_rows = len(df)
        print(f"BEFORE: Original data shape: {df.shape} ({original_rows:,} rows)")

        # STEP 0: Apply gene filtering if specified
        df = self.apply_gene_filter(df)

        # STEP 1: Apply filters
        filters = self.get_filters()
        if filters:
            mask = pd.Series([True] * len(df))

            for config_column, filter_def in filters.items():
                # Find the actual column name (handle case sensitivity)
                actual_column = None

                # First try exact match
                if config_column in df.columns:
                    actual_column = config_column
                else:
                    # Try case-insensitive match
                    for col in df.columns:
                        if col.lower() == config_column.lower():
                            actual_column = col
                            print(f"Info: Found case-insensitive match: '{config_column}' -> '{actual_column}'")
                            break

                if actual_column:
                    # Check for null/blank values before filtering
                    null_count = df[actual_column].isnull().sum() + (df[actual_column] == '').sum() + (df[actual_column] == '.').sum()

                    print(f"Applying filter to {actual_column}: {filter_def['operator']} {filter_def['value']}")
                    print(f"  Before filtering: {len(df)} total rows, {null_count} null/blank values")

                    # Show sample of the column data for debugging
                    sample_values = df[actual_column].value_counts().head(10)
                    print(f"  Sample values in {actual_column}: {dict(sample_values)}")

                    column_mask = self.apply_filter(df, actual_column, filter_def)

                    # Debug: check what's in the mask
                    print(f"  Filter mask: {column_mask.sum()} rows pass filter")

                    mask = mask & column_mask

                    remaining_count = mask.sum()
                    print(f"After filtering {actual_column} {filter_def['operator']} {filter_def['value']}: {remaining_count} rows remaining")

                    # Report null values being preserved for frequency columns
                    if 'AF' in actual_column.upper() and null_count > 0:
                        preserved_nulls = (mask & (df[actual_column].isnull() | (df[actual_column] == '') | (df[actual_column] == '.'))).sum()
                        if preserved_nulls > 0:
                            print(f"  ✓ Preserved {preserved_nulls} variants with missing frequency data")
                        else:
                            print(f"  ⚠️  WARNING: {null_count} null values existed but {preserved_nulls} were preserved!")
                else:
                    print(f"Warning: Column '{config_column}' not found in data")

            # Filter the dataframe
            df = df[mask]
            print(f"Filtered data shape: {df.shape}")

        # Drop specified columns (keep all others)
        drop_columns = self.get_columns_to_drop()
        if drop_columns:
            # Debug output: print DataFrame columns and drop_columns from config
            print("\n[DEBUG] DataFrame columns:")
            for col in df.columns:
                print(f"  '{col}'")
            print("[DEBUG] drop_columns from config:")
            for col in drop_columns:
                print(f"  '{col}'")

            # Only drop columns that actually exist in the DataFrame
            columns_to_drop = [col for col in drop_columns if col in df.columns]
            missing_drop_columns = [col for col in drop_columns if col not in df.columns]

            # Additional debug: Show exact matches
            print("\n[DEBUG] Column matching results:")
            for drop_col in drop_columns:
                if drop_col in df.columns:
                    print(f"  MATCH: '{drop_col}' will be dropped")
                else:
                    print(f"  NO MATCH: '{drop_col}' not found in DataFrame")
                    # Check for similar columns
                    similar = [col for col in df.columns if drop_col.lower() in col.lower() or col.lower() in drop_col.lower()]
                    if similar:
                        print(f"    Similar columns found: {similar}")

            print(f"\n[DEBUG] Will drop {len(columns_to_drop)} columns: {columns_to_drop}")

            if missing_drop_columns:
                print(f"Info: Drop columns not found in data: {missing_drop_columns}")

            if columns_to_drop:
                original_column_count = len(df.columns)
                df = df.drop(columns=columns_to_drop)
                print(f"Dropped {len(columns_to_drop)} columns, keeping {len(df.columns)} columns (was {original_column_count})")
            else:
                print("No columns to drop")
        else:
            print("No column dropping configured - keeping all columns")

        filtered_rows = len(df)


        # Debug: print columns present after dropping, before aggregation
        print("\n[DEBUG] Columns present after dropping, before aggregation:")
        for col in df.columns:
            print(f"  '{col}'")

        # STEP 2: Apply pivot operations
        filename = Path(zarr_path).stem
        df = self.apply_pivot_operations(df, filename, remove_validation_errors)

        final_rows = len(df)

        # Print before/after summary
        reduction_pct = ((original_rows - final_rows) / original_rows) * 100 if original_rows > 0 else 0
        print("\n📊 DATA PROCESSING SUMMARY:")
        print(f"   ORIGINAL: {original_rows:,} rows")
        print(f"   FILTERED: {filtered_rows:,} rows")
        print(f"   PIVOTED:  {final_rows:,} rows")
        print(f"   TOTAL REDUCTION: {reduction_pct:.1f}% ({original_rows - final_rows:,} rows removed)")

        # Set output path
        if output_path is None:
            output_path = str(Path(zarr_path).parent / f"{Path(zarr_path).stem}_filtered_pivoted.zarr")

        # Prepare DataFrame for xarray conversion (handle mixed types)
        print("Preparing data for Zarr conversion...")
        df_clean = self.prepare_dataframe_for_xarray(df)

        # Convert to xarray
        ds_processed = df_clean.to_xarray()

        # Add metadata
        ds_processed.attrs['original_file'] = zarr_path
        if filters:
            ds_processed.attrs['filters_applied'] = str(filters)
        ds_processed.attrs['columns_kept'] = list(df_clean.columns)
        ds_processed.attrs['original_rows'] = original_rows
        ds_processed.attrs['filtered_rows'] = filtered_rows
        ds_processed.attrs['final_rows'] = final_rows
        ds_processed.attrs['processing_type'] = 'filtered_and_pivoted'

        # Add data type information
        column_dtypes = {col: str(df_clean[col].dtype) for col in df_clean.columns}
        ds_processed.attrs['column_dtypes'] = str(column_dtypes)
        ds_processed.attrs['aggregation_note'] = 'Single values preserve original type; multiple values converted to semicolon-separated strings'

        # Save processed Zarr
        ds_processed.to_zarr(output_path, mode='w')
        print(f"Filtered and pivoted Zarr saved to: {output_path}")

        return output_path, df

    def get_available_essential_columns(self, df: pd.DataFrame) -> List[str]:
        """
        Get essential columns that are available in the dataframe.

        Args:
            df: DataFrame to check

        Returns:
            List of available essential columns (only core essential columns)
        """
        available_essential = []

        # Only check core essential columns (required for basic pivot)
        for col in self.essential_columns:
            if col in df.columns:
                available_essential.append(col)
            else:
                print(f"Warning: Core essential column '{col}' not found in data")

        if not available_essential:
            # Fallback to basic genomic coordinates if no essential columns found
            basic_columns = ['CHROM', 'POS', 'REF', 'ALT']
            available_essential = [col for col in basic_columns if col in df.columns]
            if available_essential:
                print(f"Using basic genomic coordinates for pivot: {available_essential}")

        return available_essential

    def aggregate_column_values(self, series: pd.Series) -> any:
        """
        Aggregate column values: return single value if identical, string representation if different.

        Args:
            series: pandas Series with values to aggregate

        Returns:
            Single value (preserving type) if all identical, string representation if different values exist
        """
        # Remove NaN values
        clean_values = series.dropna()

        if len(clean_values) == 0:
            return None
        elif len(clean_values) == 1:
            # Return single value with original type
            return clean_values.iloc[0]
        else:
            unique_values = clean_values.unique()
            if len(unique_values) == 1:
                # Return single value with original type
                return unique_values[0]
            else:
                # Multiple different values - convert to string representation
                # This prevents xarray mixed-type errors while preserving information
                str_values = [str(val) for val in unique_values if str(val) not in ['nan', 'None']]
                return ';'.join(str_values) if str_values else None

    def apply_pivot_operations(self, df: pd.DataFrame, filename: str, remove_validation_errors: bool = False) -> pd.DataFrame:
        """
        Apply pivot operations to convert long format to wide format.

        Args:
            df: Filtered dataframe
            filename: Source filename for metadata
            remove_validation_errors: If True, remove validation error rows and save to separate file

        Returns:
            Pivoted dataframe
        """
        print("Applying pivot operations to filtered data...")

        # Store original data for validation error reporting
        original_df = df.copy() if remove_validation_errors else None

        # Get available essential columns
        available_essential = self.get_available_essential_columns(df)

        if not available_essential:
            print("Warning: No suitable columns found for pivot operations")
            df['FILENAME'] = filename
            return df

        # Identify other columns (not in essential columns)
        all_columns = df.columns.tolist()
        other_columns = [col for col in all_columns if col not in available_essential]

        # Remove 'index' column if it exists (auto-generated by xarray/pandas conversion)
        if 'index' in other_columns:
            other_columns.remove('index')
            print("  Removing 'index' column from aggregation")

        print(f"  Essential columns for pivot: {available_essential}")
        print(f"  Other columns to aggregate: {other_columns}")

        if not other_columns:
            print("  No additional columns to aggregate")
            df['FILENAME'] = filename
            return df

        # Group by essential columns and aggregate others
        grouped = df.groupby(available_essential)

        # Prepare aggregation dictionary
        agg_dict = {}
        for col in other_columns:
            agg_dict[col] = self.aggregate_column_values

        # Perform aggregation
        print("  Performing pivot aggregation...")
        result_df = grouped.agg(agg_dict).reset_index()

        # Add filename column
        result_df['FILENAME'] = filename

        print(f"  Pivot complete: {df.shape} -> {result_df.shape}")

        # VALIDATION CHECK: Verify genomic coordinate uniqueness
        print("  Performing data integrity validation...")
        genomic_coords = ['SAMPLE', 'CHROM', 'POS', 'REF', 'ALT']
        available_genomic_coords = [col for col in genomic_coords if col in result_df.columns]

        if len(available_genomic_coords) >= 4:  # Need at least CHROM, POS, REF, ALT
            print(f"  Validating uniqueness for genomic coordinates: {available_genomic_coords}")

            # Group by genomic coordinates and count rows per group
            validation_groups = result_df.groupby(available_genomic_coords).size()
            duplicate_groups = validation_groups[validation_groups > 1]

            if len(duplicate_groups) > 0:
                print(f"  ❌ VALIDATION FAILED: Found {len(duplicate_groups)} genomic coordinate combinations with multiple rows!")

                if remove_validation_errors:
                    print("  🔧 REMOVING VALIDATION ERRORS: Extracting duplicate rows to separate file...")

                    # Find all rows that are duplicates based on genomic coordinates
                    duplicate_mask = pd.Series([False] * len(result_df))

                    for coords in duplicate_groups.index:
                        if isinstance(coords, tuple):
                            # Multi-column grouping
                            coord_dict = dict(zip(available_genomic_coords, coords))
                            mask = pd.Series([True] * len(result_df))
                            for col, val in coord_dict.items():
                                mask = mask & (result_df[col] == val)
                            duplicate_mask = duplicate_mask | mask
                        else:
                            # Single column grouping
                            duplicate_mask = duplicate_mask | (result_df[available_genomic_coords[0]] == coords)

                    # Extract duplicate coordinate combinations from pivoted data
                    error_pivoted_rows = result_df[duplicate_mask].copy()
                    print(f"  Found {len(error_pivoted_rows)} pivoted rows with validation errors")

                    # Find all original rows that contributed to these duplicate combinations
                    original_error_rows = []
                    for _, error_row in error_pivoted_rows.iterrows():
                        # Create mask to find all original rows matching this genomic coordinate combination
                        original_mask = pd.Series([True] * len(original_df))
                        for coord_col in available_genomic_coords:
                            if coord_col in original_df.columns:
                                original_mask = original_mask & (original_df[coord_col] == error_row[coord_col])

                        # Add matching original rows
                        matching_original = original_df[original_mask]
                        if len(matching_original) > 0:
                            original_error_rows.append(matching_original)

                    # Combine all original error rows
                    if original_error_rows:
                        combined_original_errors = pd.concat(original_error_rows, ignore_index=True)
                        # Remove duplicates (in case same original row contributed to multiple validation errors)
                        combined_original_errors = combined_original_errors.drop_duplicates()
                        print(f"  Found {len(combined_original_errors)} original rows that caused validation errors")
                    else:
                        combined_original_errors = pd.DataFrame()
                        print("  Warning: No original rows found for validation errors")

                    # Save validation errors to TSV file in logs directory
                    logs_dir = Path("logs")
                    logs_dir.mkdir(exist_ok=True)
                    error_filename = logs_dir / f"validation_errors_{filename}.tsv"
                    print(f"  Saving original validation error rows to: {error_filename}")

                    # Convert nullable types to standard types for TSV export
                    if len(combined_original_errors) > 0:
                        export_error_df = combined_original_errors.copy()
                        for col in export_error_df.columns:
                            if export_error_df[col].dtype.name in ['Int64', 'boolean']:
                                if export_error_df[col].dtype.name == 'Int64':
                                    export_error_df[col] = export_error_df[col].astype('float64')
                                elif export_error_df[col].dtype.name == 'boolean':
                                    export_error_df[col] = export_error_df[col].astype('object')

                        # Save to TSV
                        export_error_df.to_csv(error_filename, sep='\t', index=False, na_rep='.')
                        print(f"  ✓ Saved {len(export_error_df)} original validation error rows to {error_filename}")
                    else:
                        # Create empty file with headers if no errors found
                        pd.DataFrame(columns=original_df.columns).to_csv(error_filename, sep='\t', index=False, na_rep='.')
                        print(f"  ✓ Created empty validation error file: {error_filename}")

                    # Remove duplicate rows from result_df (pivoted data)
                    result_df = result_df[~duplicate_mask].copy()
                    print(f"  ✓ Removed validation error rows. Continuing with {len(result_df)} clean rows")

                    # Re-validate the cleaned data
                    print("  Re-validating cleaned data...")
                    validation_groups_clean = result_df.groupby(available_genomic_coords).size()
                    duplicate_groups_clean = validation_groups_clean[validation_groups_clean > 1]

                    if len(duplicate_groups_clean) > 0:
                        print(f"  ❌ ERROR: Still found {len(duplicate_groups_clean)} duplicate combinations after removal!")
                        raise ValueError("Failed to remove all validation errors")
                    else:
                        unique_combinations = len(validation_groups_clean)
                        print(f"  ✅ VALIDATION PASSED: All {unique_combinations} genomic coordinate combinations are now unique")

                else:
                    print("  Duplicate combinations with genomic coordinates:")

                    # Collect duplicate coordinate details for error message
                    duplicate_details = []
                    for i, (coords, count) in enumerate(duplicate_groups.head(20).items()):
                        if isinstance(coords, tuple):
                            coord_dict = dict(zip(available_genomic_coords, coords))
                            coord_str = ", ".join(f"{col}={val}" for col, val in coord_dict.items())
                            # Store for error message (include genomic positions)
                            genomic_info = f"CHROM={coord_dict.get('CHROM', 'N/A')} POS={coord_dict.get('POS', 'N/A')} REF={coord_dict.get('REF', 'N/A')} ALT={coord_dict.get('ALT', 'N/A')}"
                            if 'SAMPLE' in coord_dict:
                                genomic_info = f"SAMPLE={coord_dict['SAMPLE']} {genomic_info}"
                            duplicate_details.append(f"{genomic_info} ({count} rows)")
                        else:
                            coord_str = f"{available_genomic_coords[0]}={coords}"
                            duplicate_details.append(f"{coord_str} ({count} rows)")

                        print(f"    {coord_str} -> {count} rows")

                    if len(duplicate_groups) > 20:
                        print(f"    ... and {len(duplicate_groups) - 20} more duplicate combinations")

                    # Show total rows vs expected unique combinations
                    total_rows = len(result_df)
                    unique_combinations = len(validation_groups)
                    print(f"  Expected unique combinations: {unique_combinations}")
                    print(f"  Actual rows in result: {total_rows}")
                    print(f"  Extra rows due to duplicates: {total_rows - unique_combinations}")

                    # Create detailed error message with genomic coordinates
                    error_msg = (f"Data integrity validation failed: {len(duplicate_groups)} genomic coordinate combinations have multiple rows. "
                               f"Expected {unique_combinations} unique rows but found {total_rows} total rows.\n"
                               f"Duplicate genomic coordinates (showing first {min(len(duplicate_details), 10)}):\n")

                    for detail in duplicate_details[:10]:
                        error_msg += f"  - {detail}\n"

                    if len(duplicate_details) > 10:
                        error_msg += f"  ... and {len(duplicate_details) - 10} more duplicates"

                    raise ValueError(error_msg)
            else:
                unique_combinations = len(validation_groups)
                print(f"  ✅ VALIDATION PASSED: All {unique_combinations} genomic coordinate combinations are unique")
        else:
            print(f"  ⚠️  VALIDATION SKIPPED: Insufficient genomic coordinate columns found ({available_genomic_coords})")

        return result_df

    def prepare_dataframe_for_xarray(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare DataFrame for xarray conversion by ensuring consistent data types.

        Args:
            df: DataFrame to prepare

        Returns:
            DataFrame with consistent column data types
        """
        df_clean = df.copy()

        for col in df_clean.columns:
            # Check if column has mixed types (object dtype with mixed content)
            if df_clean[col].dtype == 'object':
                # Check if all non-null values are of the same type
                non_null_values = df_clean[col].dropna()
                if len(non_null_values) > 0:
                    # Get types of all non-null values
                    value_types = [type(val) for val in non_null_values]
                    unique_types = list(set(value_types))

                    # If mixed types exist, convert all to string
                    if len(unique_types) > 1:
                        print(f"  Converting column '{col}' to string due to mixed types: {unique_types}")
                        df_clean[col] = df_clean[col].astype(str)
                        df_clean[col] = df_clean[col].replace('nan', None)
                    else:
                        # Try to infer and convert to appropriate type
                        try:
                            # Check if all values are numeric
                            if all(isinstance(val, (int, float)) for val in non_null_values):
                                if all(isinstance(val, int) for val in non_null_values):
                                    df_clean[col] = df_clean[col].astype('Int64')  # Nullable integer
                                else:
                                    df_clean[col] = df_clean[col].astype('float64')
                        except (ValueError, TypeError):
                            # Keep as string if conversion fails
                            df_clean[col] = df_clean[col].astype(str)
                            df_clean[col] = df_clean[col].replace('nan', None)

        return df_clean

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Create filtered and pivoted Zarr files based on config.ini criteria')
    parser.add_argument('--zarr', '-z', required=True, help='Input Zarr file path')
    parser.add_argument('--config', '-c', default='config.ini', help='Configuration file (default: config.ini)')
    parser.add_argument('--output', '-o', help='Output Zarr file path')
    parser.add_argument('--gene-filter', '-g', help='TSV file with Gene Symbol column for filtering variants')
    parser.add_argument('--export-tsv', action='store_true', help='Also export processed data as TSV file')
    parser.add_argument('--tsv-output', help='TSV output file path (default: same as zarr output with .tsv extension)')
    parser.add_argument('--remove-validation-errors', action='store_true', help='Remove validation error rows and save them to a separate TSV file')
    parser.add_argument('--list-filters', action='store_true', help='List configured filters')
    parser.add_argument('--list-columns', action='store_true', help='List columns to keep/drop')

    args = parser.parse_args()

    # Check if config file exists
    if not Path(args.config).exists():
        print(f"Error: Configuration file {args.config} not found")
        return

    creator = ZarrFilterPivotCreator(args.config)

    # Load gene filter if specified
    if args.gene_filter:
        if not Path(args.gene_filter).exists():
            print(f"Error: Gene filter file {args.gene_filter} not found")
            return
        creator.load_gene_filter(args.gene_filter)

    # List filters if requested
    if args.list_filters:
        filters = creator.get_filters()
        if filters:
            print("Configured filters:")
            for column, filter_def in filters.items():
                print(f"  {column}: {filter_def['operator']} {filter_def['value']}")
        else:
            print("No filters configured in [FILTERS] section")
        return

    # List columns if requested
    if args.list_columns:
        drop_columns = creator.get_columns_to_drop()
        if drop_columns:
            print(f"Columns to drop ({len(drop_columns)}):")
            for col in drop_columns[:10]:  # Show first 10
                print(f"  {col}")
            if len(drop_columns) > 10:
                print(f"  ... and {len(drop_columns) - 10} more")
            print("\nNote: All other columns will be kept")
        else:
            print("No columns configured to drop - all columns will be kept")
        return

    # Validate input Zarr file
    if not Path(args.zarr).exists():
        print(f"Error: Zarr file {args.zarr} not found")
        return

    # Create filtered and pivoted Zarr
    try:
        output_path, processed_df = creator.create_filtered_pivoted_zarr(args.zarr, args.output, args.remove_validation_errors)
        print(f"✓ Successfully created filtered and pivoted Zarr: {output_path}")

        # Export TSV if requested
        if args.export_tsv:
            # Determine TSV output path
            if args.tsv_output:
                tsv_path = args.tsv_output
            else:
                # Use same base name as Zarr output but with .tsv extension
                zarr_path = Path(output_path)
                tsv_path = str(zarr_path.parent / f"{zarr_path.stem}.tsv")

            print(f"Exporting processed data to TSV: {tsv_path}")

            # Convert nullable types to standard types for TSV export
            export_df = processed_df.copy()
            for col in export_df.columns:
                if export_df[col].dtype.name in ['Int64', 'boolean']:
                    # Convert nullable types to standard types, replacing NaN with appropriate values
                    if export_df[col].dtype.name == 'Int64':
                        export_df[col] = export_df[col].astype('float64')  # Will show NaN for missing
                    elif export_df[col].dtype.name == 'boolean':
                        export_df[col] = export_df[col].astype('object')  # Will show None for missing

            # Save to TSV
            export_df.to_csv(tsv_path, sep='\t', index=False, na_rep='.')
            print(f"✓ Successfully exported TSV: {tsv_path}")

            # Report file sizes
            zarr_size_mb = sum(f.stat().st_size for f in Path(output_path).rglob('*') if f.is_file()) / (1024 * 1024)
            tsv_size_mb = Path(tsv_path).stat().st_size / (1024 * 1024)
            print("\n📁 FILE SIZES:")
            print(f"   Zarr file: {zarr_size_mb:.1f} MB")
            print(f"   TSV file: {tsv_size_mb:.1f} MB")

    except Exception as e:
        print(f"✗ Failed to create filtered and pivoted Zarr: {e}")


if __name__ == "__main__":
    main()
