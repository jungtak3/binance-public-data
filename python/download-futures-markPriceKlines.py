#!/usr/bin/env python

"""
  script to download markPriceKlines.
  set the absoluate path destination folder for STORE_DIRECTORY, and run

  e.g. STORE_DIRECTORY=/data/ ./download-kline.py

"""
import sys
from datetime import *
import pandas as pd
import numpy as np # For pandas, though direct use might be minimal with new logic
import os
from pathlib import Path
import zipfile # Added for unzipping
import fgrequests
import re # Added for regex in filename parsing

from enums import START_DATE, END_DATE, DAILY_INTERVALS, PERIOD_START_DATE
from utility import (
    get_all_symbols, get_parser, convert_to_date_object,
    get_path, raise_arg_error, get_destination_dir, get_download_url
)

# Helper function to save content downloaded by fgrequests
def save_response_content(response, save_path, url_for_logging):
    if response and response.status_code == 200:
        try:
            Path(os.path.dirname(save_path)).mkdir(parents=True, exist_ok=True)
            with open(save_path, 'wb') as f:
                f.write(response.content)
            print(f"\nSuccessfully downloaded {url_for_logging} to {save_path}")
        except Exception as e:
            print(f"\nError saving {url_for_logging} to {save_path}: {e}")
    elif response:
        print(f"\nFailed to download {url_for_logging}. Status: {response.status_code} ({response.reason})")
    else:
        # This case for fgrequests.map returning None for exceptions during request (e.g. connection error)
        print(f"\nFailed to download {url_for_logging} (no response or exception during request).")


def unzip_specific_csv(zip_filepath, extract_to_directory):
    """
    Unzips a single CSV file from a zip archive.
    Assumes the zip file contains one primary CSV data file.
    The CSV is extracted directly into extract_to_directory.
    Returns the full path to the extracted CSV file, or None on failure.
    """
    try:
        Path(extract_to_directory).mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
            csv_member_names = [m.filename for m in zip_ref.infolist() if not m.is_dir() and m.filename.lower().endswith('.csv')]
            if not csv_member_names:
                print(f"Warning: No CSV file found in {zip_filepath}")
                return None
            
            csv_member_name_in_zip = csv_member_names[0]
            
            source = zip_ref.open(csv_member_name_in_zip)
            target_filename = Path(csv_member_name_in_zip).name
            target_path = Path(extract_to_directory) / target_filename
            
            with open(target_path, "wb") as f:
                f.write(source.read())
            source.close()
            
            print(f"Successfully extracted {csv_member_name_in_zip} from {zip_filepath} to {target_path}")
            return str(target_path)
    except zipfile.BadZipFile:
        print(f"Error: Bad zip file {zip_filepath}")
        return None
    except Exception as e:
        print(f"Error unzipping {zip_filepath}: {e}")
        return None

# Helper function to parse date from filename for sorting
def get_sortable_date_from_filename(filepath_str):
    filename = Path(filepath_str).name
    # Daily pattern: SYMBOL-INTERVAL-YYYY-MM-DD.csv
    daily_match = re.search(r'-(\d{4})-(\d{2})-(\d{2})\.csv$', filename)
    if daily_match:
        year, month, day = map(int, daily_match.groups())
        return datetime(year, month, day)
    
    # Monthly pattern: SYMBOL-INTERVAL-YYYY-MM.csv
    monthly_match = re.search(r'-(\d{4})-(\d{2})\.csv$', filename)
    if monthly_match:
        year, month = map(int, monthly_match.groups())
        return datetime(year, month, 1) # Use 1st day for month sorting consistency
    
    print(f"Warning: Could not parse date from filename: {filename}. Using minimum date for sorting.")
    return datetime.min # Fallback for sorting, will be sorted first

def merge_symbol_csvs(symbol, csv_file_paths, output_directory):
    """
    Merges multiple CSV files for a given symbol into a single CSV file.
    The column order of the merged file is determined by the most recent CSV file.
    Sorts data by timestamp (assumed to be the first column of the canonical header).
    Saves the merged file with a name including the symbol and date range.
    """
    if not csv_file_paths:
        print(f"No CSV files to merge for symbol {symbol}.")
        return

    # 1. Find the most recent CSV to determine canonical header
    try:
        sorted_csv_file_paths = sorted(csv_file_paths, key=get_sortable_date_from_filename, reverse=True)
        if not sorted_csv_file_paths:
             print(f"Error: No sortable CSV files found for {symbol} after attempting to parse dates.")
             return
        most_recent_csv_path = sorted_csv_file_paths[0]
    except Exception as e:
        print(f"Error determining most recent CSV for {symbol}: {e}. Files: {csv_file_paths}")
        return

    # 2. Read the header from this "most recent" CSV file.
    canonical_header = []
    try:
        # Read only the header row to get column names
        canonical_header_df = pd.read_csv(most_recent_csv_path, nrows=0)
        canonical_header = canonical_header_df.columns.tolist()
        if not canonical_header:
            print(f"Error: Most recent CSV '{most_recent_csv_path}' has no header or is empty. Cannot merge for {symbol}.")
            return
    except pd.errors.EmptyDataError:
        print(f"Error: Most recent CSV '{most_recent_csv_path}' is empty (EmptyDataError). Cannot determine header for {symbol}.")
        return
    except Exception as e:
        print(f"Error reading header from {most_recent_csv_path}: {e}. Cannot merge for {symbol}.")
        return

    all_dataframes = []
    for csv_path in csv_file_paths: # Iterate original or sorted, order doesn't matter for concat
        try:
            # Read with header=0 so pandas uses the first row as header
            df = pd.read_csv(csv_path, header=0)
            if df.empty:
                print(f"Warning: Empty CSV file skipped: {csv_path}")
                continue
            all_dataframes.append(df)
        except pd.errors.EmptyDataError:
            print(f"Warning: Empty CSV file skipped (EmptyDataError): {csv_path}")
        except Exception as e:
            print(f"Error reading CSV file {csv_path}: {e}. Skipping this file for {symbol}.")
            # Optionally, one could choose to 'return' here to stop merging for the symbol on any error.
            # For now, we skip the problematic file and try to merge the rest.
            continue

    if not all_dataframes:
        print(f"No dataframes could be read or all were empty for symbol {symbol}.")
        return

    try:
        # Concatenate. Pandas will create a union of columns.
        merged_df = pd.concat(all_dataframes, ignore_index=True)
        
        # Re-order columns to match the canonical header from the most recent file.
        # Columns in canonical_header not in merged_df will be added as NaN.
        # Columns in merged_df not in canonical_header will be dropped.
        merged_df = merged_df.reindex(columns=canonical_header)

        # Assume the first column of the canonical_header is the timestamp column
        if not canonical_header: # Should have been caught earlier, but as a safeguard
            print(f"Critical error: Canonical header is empty for {symbol} before timestamp processing.")
            return
            
        timestamp_col_name = canonical_header[0]

        merged_df[timestamp_col_name] = pd.to_numeric(merged_df[timestamp_col_name])
        merged_df.sort_values(by=timestamp_col_name, inplace=True)

        min_timestamp_ms = merged_df[timestamp_col_name].min()
        max_timestamp_ms = merged_df[timestamp_col_name].max()

        min_date = pd.to_datetime(min_timestamp_ms, unit='ms')
        max_date = pd.to_datetime(max_timestamp_ms, unit='ms')

        start_date_str = min_date.strftime('%Y%m%d')
        end_date_str = max_date.strftime('%Y%m%d')

        output_filename = f"{symbol.upper()}_{start_date_str}_{end_date_str}.csv"
        output_path = Path(output_directory) / output_filename
        
        Path(output_directory).mkdir(parents=True, exist_ok=True)

        # Save with header=True
        merged_df.to_csv(output_path, index=False, header=True)
        print(f"Successfully merged {len(all_dataframes)} CSVs for {symbol} into {output_path} with columns from {Path(most_recent_csv_path).name}")

        # Optional: Cleanup extracted CSV files (now in output_directory)
        # for csv_path_to_delete in csv_file_paths: # These are the paths of the extracted (now source) CSVs
        #     try:
        #         Path(csv_path_to_delete).unlink()
        #         print(f"Deleted source extracted file: {csv_path_to_delete}")
        #     except OSError as e:
        #         print(f"Error deleting source extracted file {csv_path_to_delete}: {e}")

    except Exception as e:
        print(f"Error during final merge, sort, or save step for symbol {symbol}: {e}")


def download_monthly_markPriceKlines(trading_type, symbols, num_symbols, intervals, years, months, start_date_str,
                                      end_date_str, folder, checksum, all_extracted_csvs_map):
    if folder is None:
        folder = "./downloaded_markprice_klines"
        print(f"Warning: --folder argument not provided for monthly downloads. Using default: {folder}")
    current = 0
    date_range = None

    if start_date_str and end_date_str:
        date_range = start_date_str + " " + end_date_str

    actual_start_date = convert_to_date_object(start_date_str) if start_date_str else START_DATE
    if isinstance(actual_start_date, str) :
        actual_start_date = convert_to_date_object(actual_start_date)

    actual_end_date = convert_to_date_object(end_date_str) if end_date_str else END_DATE
    if isinstance(actual_end_date, str) :
        actual_end_date = convert_to_date_object(actual_end_date)

    print("Found {} symbols".format(num_symbols))

    for symbol in symbols:
        print("[{}/{}] - start download monthly {} markPriceKlines ".format(current + 1, num_symbols, symbol))
        files_to_download_info_for_symbol = []
        for interval in intervals:
            for year in years:
                for month in months:
                    current_file_date = convert_to_date_object('{}-{}-01'.format(year, month))
                    if actual_start_date <= current_file_date <= actual_end_date:
                        base_path = get_path(trading_type, "markPriceKlines", "monthly", symbol, interval)
                        file_name = "{}-{}-{}-{}.zip".format(symbol.upper(), interval, year, '{:02d}'.format(month))

                        effective_save_base = base_path
                        if date_range:
                            effective_save_base = os.path.join(effective_save_base, date_range.replace(" ", "_"))
                        save_path = get_destination_dir(os.path.join(effective_save_base, file_name), folder)

                        if os.path.exists(save_path):
                            print(f"\nFile already exists: {save_path}")
                        else:
                            download_uri = os.path.join(base_path, file_name)
                            full_download_url = get_download_url(download_uri)
                            files_to_download_info_for_symbol.append((full_download_url, save_path))

                        if checksum == 1:
                            checksum_file_name = "{}-{}-{}-{}.zip.CHECKSUM".format(symbol.upper(), interval, year, '{:02d}'.format(month))
                            checksum_save_path = get_destination_dir(os.path.join(effective_save_base, checksum_file_name), folder)

                            if os.path.exists(checksum_save_path):
                                print(f"\nFile already exists: {checksum_save_path}")
                            else:
                                checksum_download_uri = os.path.join(base_path, checksum_file_name)
                                full_checksum_download_url = get_download_url(checksum_download_uri)
                                files_to_download_info_for_symbol.append((full_checksum_download_url, checksum_save_path))

        if files_to_download_info_for_symbol:
            print(f"Attempting to download {len(files_to_download_info_for_symbol)} files for {symbol}...")
            urls_to_fetch = [info[0] for info in files_to_download_info_for_symbol]
            responses = fgrequests.build(urls_to_fetch, max_retries=2)

            for i, response_item in enumerate(responses):
                downloaded_url, file_save_path = files_to_download_info_for_symbol[i]
                save_response_content(response_item, file_save_path, downloaded_url)

                # Unzip if download was successful and it's a zip file (not CHECKSUM)
                if response_item and response_item.status_code == 200 and \
                  file_save_path.lower().endswith('.zip') and \
                  not file_save_path.lower().endswith('.zip.checksum'):
                   
                   # Extract directly into the main output folder
                   # 'folder' is the main output directory (e.g., "./downloaded_markprice_klines")
                   extracted_csv_file = unzip_specific_csv(file_save_path, str(folder))
                   
                   if extracted_csv_file:
                       all_extracted_csvs_map.setdefault(symbol.upper(), []).append(extracted_csv_file)
                        # Optional: Delete the original zip file
                        # try:
                        #     Path(file_save_path).unlink()
                        #     print(f"Deleted original zip file: {file_save_path}")
                        # except OSError as e:
                        #     print(f"Error deleting zip file {file_save_path}: {e}")
        else:
            print(f"No new files to download for {symbol} within the specified criteria.")

        current += 1


def download_daily_markPriceKlines(trading_type, symbols, num_symbols, intervals, dates, start_date_str, end_date_str, folder,
                                     checksum, all_extracted_csvs_map):
    if folder is None:
        folder = "./downloaded_markprice_klines"
        print(f"Warning: --folder argument not provided for daily downloads. Using default: {folder}")
    current = 0
    date_range = None

    if start_date_str and end_date_str:
        date_range = start_date_str + " " + end_date_str

    actual_start_date = convert_to_date_object(start_date_str) if start_date_str else START_DATE
    if isinstance(actual_start_date, str):
        actual_start_date = convert_to_date_object(actual_start_date)

    actual_end_date = convert_to_date_object(end_date_str) if end_date_str else END_DATE
    if isinstance(actual_end_date, str):
        actual_end_date = convert_to_date_object(actual_end_date)

    valid_intervals = list(set(intervals) & set(DAILY_INTERVALS))
    if not valid_intervals:
        print("No valid daily intervals selected or available for daily downloads.")
        return

    print("Found {} symbols".format(num_symbols))

    for symbol in symbols:
        print("[{}/{}] - start download daily {} markPriceKlines ".format(current + 1, num_symbols, symbol))
        files_to_download_info_for_symbol = []
        for interval in valid_intervals:
            for date_str in dates:
                current_file_date = convert_to_date_object(date_str)
                if actual_start_date <= current_file_date <= actual_end_date:
                    base_path = get_path(trading_type, "markPriceKlines", "daily", symbol, interval)
                    file_name = "{}-{}-{}.zip".format(symbol.upper(), interval, date_str)

                    effective_save_base = base_path
                    if date_range:
                        effective_save_base = os.path.join(effective_save_base, date_range.replace(" ", "_"))
                    save_path = get_destination_dir(os.path.join(effective_save_base, file_name), folder)

                    if os.path.exists(save_path):
                        print(f"\nFile already exists: {save_path}")
                    else:
                        download_uri = os.path.join(base_path, file_name)
                        full_download_url = get_download_url(download_uri)
                        files_to_download_info_for_symbol.append((full_download_url, save_path))

                    if checksum == 1:
                        checksum_file_name = "{}-{}-{}.zip.CHECKSUM".format(symbol.upper(), interval, date_str)
                        checksum_save_path = get_destination_dir(os.path.join(effective_save_base, checksum_file_name), folder)

                        if os.path.exists(checksum_save_path):
                            print(f"\nFile already exists: {checksum_save_path}")
                        else:
                            checksum_download_uri = os.path.join(base_path, checksum_file_name)
                            full_checksum_download_url = get_download_url(checksum_download_uri)
                            files_to_download_info_for_symbol.append((full_checksum_download_url, checksum_save_path))

        if files_to_download_info_for_symbol:
            print(f"Attempting to download {len(files_to_download_info_for_symbol)} files for {symbol}...")
            urls_to_fetch = [info[0] for info in files_to_download_info_for_symbol]
            responses = fgrequests.build(urls_to_fetch, max_retries=2)

            for i, response_item in enumerate(responses):
                downloaded_url, file_save_path = files_to_download_info_for_symbol[i]
                save_response_content(response_item, file_save_path, downloaded_url)

                # Unzip if download was successful and it's a zip file (not CHECKSUM)
                if response_item and response_item.status_code == 200 and \
                  file_save_path.lower().endswith('.zip') and \
                  not file_save_path.lower().endswith('.zip.checksum'):

                   # Extract directly into the main output folder
                   # 'folder' is the main output directory (e.g., "./downloaded_markprice_klines")
                   extracted_csv_file = unzip_specific_csv(file_save_path, str(folder))

                   if extracted_csv_file:
                       all_extracted_csvs_map.setdefault(symbol.upper(), []).append(extracted_csv_file)
                        # Optional: Delete the original zip file
                        # try:
                        #     Path(file_save_path).unlink()
                        #     print(f"Deleted original zip file: {file_save_path}")
                        # except OSError as e:
                        #     print(f"Error deleting zip file {file_save_path}: {e}")
        else:
            print(f"No new files to download for {symbol} within the specified criteria.")

        current += 1


if __name__ == "__main__":
    parser = get_parser('klines')
    args = parser.parse_args(sys.argv[1:])

    if args.type == 'spot':
        raise_arg_error('Spot not supported for markPriceKlines. Valid Types: um, cm')

    if not args.symbols:
        print("fetching all symbols from exchange")
        symbols = get_all_symbols(args.type)
        num_symbols = len(symbols)
    else:
        symbols = args.symbols
        num_symbols = len(symbols)

    all_extracted_csvs_map = {} # Initialize dict to store paths of extracted CSVs

    dates_to_process = args.dates
    run_monthly = not args.dates

    if not dates_to_process:
        today_date_obj = convert_to_date_object(datetime.today().strftime('%Y-%m-%d'))
        period_start_date_obj = convert_to_date_object(PERIOD_START_DATE)
        period_days = (today_date_obj - period_start_date_obj).days

        dates_to_process = pd.date_range(end=datetime.today(), periods=period_days + 1).to_pydatetime().tolist()
        dates_to_process = [date_obj.strftime("%Y-%m-%d") for date_obj in dates_to_process]

    if run_monthly:
        print("Proceeding with monthly markPriceKlines downloads...")
        download_monthly_markPriceKlines(args.type, symbols, num_symbols, args.intervals, args.years, args.months,
                                          args.startDate, args.endDate, args.folder, args.checksum,
                                          all_extracted_csvs_map)
    else:
        print("Skipping monthly markPriceKlines downloads because specific dates were provided via -d.")

    print("Proceeding with daily markPriceKlines downloads...")
    download_daily_markPriceKlines(args.type, symbols, num_symbols, args.intervals, dates_to_process, args.startDate,
                                    args.endDate, args.folder, args.checksum,
                                    all_extracted_csvs_map)

    # After all downloads, merge CSVs
    if all_extracted_csvs_map:
        print("\nStarting CSV merging process...")
        output_dir_for_merged_files = args.folder
        if output_dir_for_merged_files is None:
            output_dir_for_merged_files = "./downloaded_markprice_klines"
            # Print warning only if it's truly a new default being set here,
            # though args.folder would likely be the source if None.
            # This ensures merge output directory is also defaulted.
            if args.folder is None: # Check original source of None
                 print(f"Warning: --folder not specified for merged output. Using default: {output_dir_for_merged_files}")
        # Main output directory for final merged files
        for symbol_key, csv_paths_list in all_extracted_csvs_map.items():
            if csv_paths_list:
                merge_symbol_csvs(symbol_key, csv_paths_list, output_dir_for_merged_files)
            else:
                print(f"No extracted CSVs found for symbol {symbol_key} to merge.")
        print("CSV merging process complete.")
        
        # The temp_extracted_data directory is no longer used for extraction.
        # Individual extracted CSVs are now in the main output folder (args.folder).
        # Cleanup for those (if desired) would be handled by uncommenting lines
        # within the merge_symbol_csvs function.
    else:
        print("\nNo files were extracted; skipping merging process.")
