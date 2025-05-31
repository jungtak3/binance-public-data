#!/usr/bin/env python

"""
Merges multiple CSV files belonging to the same trading symbol into a single
CSV file per symbol. It intelligently determines column headers from the
most recent file for each symbol.
"""

import os
import sys
import argparse
from pathlib import Path
import pandas as pd
from datetime import date, timedelta, datetime # datetime might be needed by pandas or for strptime if used
import re
import numpy as np
import glob # For Path.glob or direct glob usage
from collections import defaultdict

# --- Functions copied from download-kline.py ---

def parse_date_from_filename(filename):
    """
    Parses date from kline CSV filenames.
    Example filenames: SYMBOL-INTERVAL-YYYY-MM-DD.csv or SYMBOL-INTERVAL-YYYY-MM.csv
    Returns a datetime.date object.
    """
    name_part = Path(filename).stem # Remove .csv
    parts = name_part.split('-')
    if len(parts) >= 4: # Potentially YYYY-MM-DD or YYYY-MM
        date_str_parts = parts[-3:] # e.g. ['2023', '01', '15'] or ['1m', '2023', '01']
        if len(date_str_parts[0]) == 4 and date_str_parts[0].isdigit(): # YYYY-MM-DD or YYYY-MM
            year = int(date_str_parts[0])
            month = int(date_str_parts[1])
            if len(date_str_parts) == 3 and date_str_parts[2].isdigit() and len(date_str_parts[2]) == 2: # YYYY-MM-DD
                day = int(date_str_parts[2])
                return date(year, month, day)
            # elif len(date_str_parts) == 2 : # YYYY-MM, original logic from download-kline
            # For monthly, use last day of month to compare recency against daily files
            #    next_month = date(year, month, 1).replace(day=28) + timedelta(days=4)
            #    return next_month - timedelta(days=next_month.day)
            # Corrected logic for YYYY-MM, assuming it's the first day for simplicity or requires more context
            # For this script, we'll stick to the original logic if it was intended for sorting monthly vs daily
            # However, the example SYMBOL-INTERVAL-YYYY-MM.csv implies the date part is YYYY-MM.
            # The original code had a specific way to handle this for monthly files.
            # Let's refine this part slightly if it's just about parsing, not comparing monthly vs daily specifically.
            # For now, keeping original logic for monthly files if that's the intent.
            elif len(parts) == 4 and parts[1].endswith('m') and parts[0].isalpha(): # Heuristic for SYMBOL-INTERVAL-YYYY-MM
                 # This case might be specific to how download-kline names monthly zips/csvs
                 # e.g. BTCUSDT-1m-2023-01.csv
                 try:
                     year_from_part = int(parts[2])
                     month_from_part = int(parts[3])
                     # Use last day of month for sorting recency as in original
                     next_month_dt = date(year_from_part, month_from_part, 1).replace(day=28) + timedelta(days=4)
                     return next_month_dt - timedelta(days=next_month_dt.day)
                 except ValueError:
                     pass # Fall through to regex

    # Fallback for filenames like SYMBOL-1d-2024-05-18.csv (extracted from zip)
    # Or SYMBOL-1m-2024-01.csv
    # Try to match YYYY-MM-DD or YYYY-MM pattern
    match_daily = re.search(r'(\d{4})-(\d{2})-(\d{2})', name_part)
    if match_daily:
        return date(int(match_daily.group(1)), int(match_daily.group(2)), int(match_daily.group(3)))
    
    match_monthly = re.search(r'(\d{4})-(\d{2})', name_part)
    if match_monthly:
        year = int(match_monthly.group(1))
        month = int(match_monthly.group(2))
        # Use last day of month to compare recency as in original
        next_month_dt = date(year, month, 1).replace(day=28) + timedelta(days=4)
        return next_month_dt - timedelta(days=next_month_dt.day)
        
    print(f"Warning: Could not parse date from filename: {filename}. Using epoch for sorting.")
    return date(1970, 1, 1)


def merge_symbol_klines_csvs(symbol, csv_file_paths, output_directory):
    if not csv_file_paths:
        print(f"No CSV files provided for symbol {symbol}. Skipping merge.")
        return

    # A. Canonical Header Determination
    # Sort csv_file_paths by date parsed from filename to find the most recent
    sorted_file_paths_for_header = sorted(
        csv_file_paths,
        key=lambda p: parse_date_from_filename(Path(p).name),
        reverse=True  # Most recent first
    )

    canonical_header = None
    latest_file_for_header = None

    for f_path_str in sorted_file_paths_for_header:
        try:
            latest_file_for_header = Path(f_path_str)
            if not latest_file_for_header.exists() or latest_file_for_header.stat().st_size == 0:
                print(f"Warning: Header candidate file {latest_file_for_header.name} is empty or does not exist. Trying next.")
                latest_file_for_header = None # Reset to try next
                continue
            
            # Read only the header row
            header_df = pd.read_csv(latest_file_for_header, nrows=0, low_memory=False)
            if not header_df.columns.empty:
                canonical_header = header_df.columns.tolist()
                print(f"Using header from {latest_file_for_header.name} as canonical for {symbol}: {canonical_header}")
                break
            else:
                print(f"Warning: Header candidate file {latest_file_for_header.name} has no columns. Trying next.")
                latest_file_for_header = None # Reset

        except pd.errors.EmptyDataError:
            print(f"Warning: EmptyDataError for header candidate file {latest_file_for_header.name if latest_file_for_header else f_path_str}. Trying next.")
            latest_file_for_header = None # Reset
        except Exception as e:
            print(f"Error reading header from {latest_file_for_header.name if latest_file_for_header else f_path_str}: {e}. Trying next.")
            latest_file_for_header = None # Reset

    if canonical_header is None:
        print(f"Critical: Could not determine a canonical header for symbol {symbol} after checking all files. Skipping merge for this symbol.")
        return

    # B. Processing Individual CSVs & Data Collection
    data_to_merge = []  # List of (file_date, dataframe) tuples

    for f_path_str in csv_file_paths: # Process all files, not necessarily sorted here
        f_path = Path(f_path_str)
        if not f_path.exists():
            print(f"Warning: File not found {f_path_str}, skipping.")
            continue
        
        file_date = parse_date_from_filename(f_path.name)

        try:
            has_header = False
            if f_path.stat().st_size > 0: # Check if file is not empty
                with open(f_path, 'r', encoding='utf-8') as f_peek:
                    first_line = f_peek.readline().strip()
                    if first_line: # Ensure first line is not empty
                        first_field = first_line.split(',')[0]
                        try:
                            # Attempt to convert the first field to a number
                            float(first_field)
                            # If successful, it's likely data, not a header
                            has_header = False
                        except ValueError:
                            # If ValueError, it's likely a header string
                            has_header = True
            else: # File is empty
                print(f"Skipping empty file: {f_path_str}")
                continue

            df = None
            if has_header:
                df = pd.read_csv(f_path, low_memory=False, dtype=str) # Read all as str initially
                if df.empty:
                    print(f"Skipping empty dataframe from (header-detected) file: {f_path_str}")
                    continue
                
                # Align columns to canonical_header
                # Create a new DataFrame with canonical_header and fill from df
                aligned_df = pd.DataFrame(columns=canonical_header, dtype=str)
                for col in canonical_header:
                    if col in df.columns:
                        aligned_df[col] = df[col]
                    else:
                        aligned_df[col] = pd.NA # Use pandas NA for missing
                df = aligned_df
            else: # No header detected or file was empty initially
                df = pd.read_csv(f_path, header=None, names=canonical_header, low_memory=False, dtype=str)
                if df.empty:
                    print(f"Skipping empty dataframe from (no-header-detected) file: {f_path_str}")
                    continue
            
            data_to_merge.append((file_date, df))

        except pd.errors.EmptyDataError:
            print(f"Warning: Empty CSV file encountered: {f_path_str}, skipping.")
            continue
        except Exception as e:
            print(f"Error processing CSV {f_path_str}: {e}, skipping.")
            continue

    # C. Sorting, Concatenation, Final Processing
    if not data_to_merge:
        print(f"No data collected to merge for symbol {symbol}. Skipping.")
        return

    # Sort data by file_date (ascending) before concatenation
    data_to_merge.sort(key=lambda x: x[0])

    # Concatenate all dataframes
    merged_df = pd.concat([item[1] for item in data_to_merge], ignore_index=True)

    if merged_df.empty:
        print(f"Merged dataframe for {symbol} is empty. Skipping save.")
        return

    if not merged_df.columns.tolist() == canonical_header: # Should match due to earlier alignment
        print(f"Warning: Merged DF columns {merged_df.columns.tolist()} do not match canonical {canonical_header} for {symbol}. This is unexpected.")
        # Attempt to re-align, though this indicates a logic flaw if it happens.
        final_df_for_save = pd.DataFrame(columns=canonical_header)
        for col in canonical_header:
            if col in merged_df.columns:
                final_df_for_save[col] = merged_df[col]
            else:
                final_df_for_save[col] = pd.NA
        merged_df = final_df_for_save


    timestamp_col_name = canonical_header[0] # Use the first column from canonical_header

    # Convert timestamp column to numeric, then to datetime
    # Coerce errors: invalid parsing will be set as NaT
    merged_df[timestamp_col_name] = pd.to_numeric(merged_df[timestamp_col_name], errors='coerce')
    merged_df.dropna(subset=[timestamp_col_name], inplace=True) # Drop rows where conversion to numeric failed

    if merged_df.empty:
        print(f"Dataframe for {symbol} is empty after numeric conversion of timestamp. Skipping.")
        return

    # Convert to datetime, assuming milliseconds Unix timestamp
    merged_df[timestamp_col_name] = pd.to_datetime(merged_df[timestamp_col_name], unit='ms', errors='coerce')
    merged_df.dropna(subset=[timestamp_col_name], inplace=True) # Drop rows where timestamp conversion failed (NaT)

    if merged_df.empty:
        print(f"Dataframe for {symbol} is empty after datetime conversion/dropna. Skipping save.")
        return

    # Sort by the timestamp column (ascending)
    merged_df.sort_values(by=timestamp_col_name, inplace=True)

    # Deduplicate based on the timestamp column, keeping the first occurrence
    merged_df.drop_duplicates(subset=[timestamp_col_name], keep='first', inplace=True)

    if merged_df.empty: # Check again after deduplication
        print(f"Dataframe for {symbol} is empty after deduplication. Skipping save.")
        return

    # Derive min_date_str and max_date_str from the processed timestamp column
    min_date_dt = merged_df[timestamp_col_name].min()
    max_date_dt = merged_df[timestamp_col_name].max()
    min_date_str = min_date_dt.strftime('%Y%m%d')
    max_date_str = max_date_dt.strftime('%Y%m%d')

    # Convert timestamp column back to int64 (milliseconds epoch)
    # Timestamps are already datetime objects, convert to Unix time in ms
    merged_df[timestamp_col_name] = (merged_df[timestamp_col_name].astype(np.int64) // 10**6)

    output_filename = f"{symbol.upper()}_{min_date_str}_{max_date_str}.csv"
    output_file_path = Path(output_directory) / output_filename

    try:
        # Save with header=True, which will use the DataFrame's column names (the canonical_header)
        merged_df.to_csv(output_file_path, index=False, header=True)
        print(f"Successfully merged {len(csv_file_paths)} initial files (processed {len(data_to_merge)} non-empty dataframes) for {symbol} into: {output_file_path}")
    except Exception as e:
        print(f"Error saving merged CSV for {symbol} to {output_file_path}: {e}")

# --- End of copied functions ---


def main():
    parser = argparse.ArgumentParser(
        description="Merge multiple CSV files for the same trading symbol into a single CSV per symbol."
    )
    parser.add_argument(
        "--input_dir",
        type=str,
        required=True,
        help="Directory to scan for input CSV files. This directory should contain subdirectories named after symbols."
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        required=True,
        help="Directory where the merged CSV files will be saved."
    )
    parser.add_argument(
        "--recursive",
        action='store_true',
        default=False,
        help="If specified, scan each symbol's subdirectory recursively for CSV files."
    )
    parser.add_argument(
        "--file_pattern",
        type=str,
        default="*.csv",
        help="Glob pattern to use for finding CSV files within each symbol's subdirectory (e.g., \"*.csv\", \"DATA-*.csv\"). Default is \"*.csv\"."
    )
    args = parser.parse_args()

    input_path = Path(args.input_dir)
    output_path = Path(args.output_dir)

    if not input_path.is_dir():
        print(f"Error: Input directory '{args.input_dir}' does not exist or is not a directory.")
        sys.exit(1)

    # Ensure output directory exists
    print(f"Ensuring output directory exists: {output_path}")
    output_path.mkdir(parents=True, exist_ok=True)

    files_by_symbol = defaultdict(list)
    print(f"Scanning for symbol subdirectories in '{input_path}'...")

    found_any_symbol_dirs = False
    for item in input_path.iterdir():
        if item.is_dir():
            found_any_symbol_dirs = True
            symbol = item.name
            symbol_dir_path = item

            print(f"Processing symbol directory: '{symbol}'. Searching for files with pattern '{args.file_pattern}' (recursive: {args.recursive})...")

            if args.recursive:
                csv_files_in_symbol_dir = list(symbol_dir_path.rglob(args.file_pattern))
            else:
                csv_files_in_symbol_dir = list(symbol_dir_path.glob(args.file_pattern))
            
            current_symbol_file_count = 0
            if csv_files_in_symbol_dir:
                for f_path in csv_files_in_symbol_dir:
                    if f_path.is_file():
                        files_by_symbol[symbol].append(str(f_path))
                        current_symbol_file_count +=1
            
            if current_symbol_file_count > 0:
                 print(f"Found {current_symbol_file_count} files for symbol '{symbol}'.")
            else:
                print(f"No files matching pattern '{args.file_pattern}' found for symbol '{symbol}' in '{symbol_dir_path}'.")

    if not found_any_symbol_dirs:
        print(f"No subdirectories found in '{input_path}'. Ensure input directory contains symbol subdirectories.")
        sys.exit(0)
        
    if not files_by_symbol:
        print("No CSV files found within any symbol subdirectories matching the pattern and criteria.")
        sys.exit(0)
    
    total_csv_files_found = sum(len(files) for files in files_by_symbol.values())
    print(f"\nFound a total of {total_csv_files_found} CSV files across {len(files_by_symbol)} symbol subdirectories.")

    print(f"\nStarting merge process for {len(files_by_symbol)} symbols...")
    for symbol, file_list in files_by_symbol.items():
        print(f"\nFound {len(file_list)} files for symbol {symbol.upper()}.")
        print(f"Merging files for symbol {symbol.upper()}...")
        merge_symbol_klines_csvs(symbol.upper(), file_list, str(output_path))

    print("\nScript finished.")

if __name__ == "__main__":
    main()