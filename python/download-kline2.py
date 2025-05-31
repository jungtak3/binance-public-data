#!/usr/bin/env python

"""
  script to download klines.
  set the absolute path destination folder for STORE_DIRECTORY, and run

  e.g. STORE_DIRECTORY=/data/ ./download-kline.py

"""
import sys
import os
from datetime import *
import pandas as pd
import fgrequests
import zipfile
from pathlib import Path
from enums import *
import re
import numpy as np
from utility import get_all_symbols, get_parser, get_start_end_date_objects, convert_to_date_object, \
  get_path, get_download_url, get_destination_dir # Removed download_file, added get_download_url, get_destination_dir

DEFAULT_OUTPUT_FOLDER = "./downloaded_klines"
CHUNK_SIZE = 300 # Max requests per batch for fgrequests

CANONICAL_KLINES_HEADERS = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'count', 'taker_buy_volume', 'taker_buy_quote_volume', 'ignore']

def save_response_content(response, destination_path, symbol, interval, extracted_files_map):
    """Saves the content of a successful fgrequests response and unzips if it's a zip file."""
    try:
        # Ensure the directory exists
        Path(destination_path).parent.mkdir(parents=True, exist_ok=True)

        with open(destination_path, 'wb') as f:
            f.write(response.content)
        print(f"Saved: {destination_path}")

        if destination_path.endswith(".zip"):
            with zipfile.ZipFile(destination_path, 'r') as zip_ref:
                unzip_dir = Path(destination_path).parent
                for member in zip_ref.namelist():
                    if member.endswith('.csv'): # Assuming one relevant CSV per zip
                        zip_ref.extract(member, unzip_dir)
                        extracted_csv_path = str(unzip_dir / member)
                        print(f"Unzipped: {extracted_csv_path}")
                        if symbol not in extracted_files_map:
                            extracted_files_map[symbol] = []
                        extracted_files_map[symbol].append(extracted_csv_path)
            # After successful extraction from the zip, remove the zip file
            try:
                os.remove(destination_path)
                print(f"Removed zip file: {destination_path}")
            except OSError as e:
                print(f"Error removing zip file {destination_path}: {e}")
                        
    except Exception as e:
        print(f"Error saving/unzipping {destination_path} for {symbol}: {e}")


def download_monthly_klines(trading_type, symbols, num_symbols, intervals, years, months, start_date, end_date, folder, checksum):
  current = 0
  if folder is None:
    folder = DEFAULT_OUTPUT_FOLDER
  Path(folder).mkdir(parents=True, exist_ok=True)

  extracted_files_map_for_function = {}

  if not start_date:
    start_date_obj = START_DATE
  else:
    start_date_obj = convert_to_date_object(start_date)

  if not end_date:
    end_date_obj = END_DATE
  else:
    end_date_obj = convert_to_date_object(end_date)

  print("Found {} symbols for monthly download".format(num_symbols))
  
  all_requests_metadata = [] # Changed name from all_requests to match user's conceptual flow

  for symbol in symbols:
    print(f"[{current+1}/{num_symbols}] - Preparing monthly {symbol} klines for download")
    effective_start_date_obj_for_symbol = start_date_obj
    output_folder_path = Path(folder)
    merged_file_pattern = f"{symbol.upper()}_*_*.csv"
    latest_merged_end_date = None
    for merged_file in output_folder_path.glob(merged_file_pattern):
        try:
            symbol_prefix_in_filename = symbol.upper() + "_"
            if not merged_file.stem.startswith(symbol_prefix_in_filename):
                continue
            dates_section = merged_file.stem[len(symbol_prefix_in_filename):]
            date_strings = dates_section.split('_')
            if len(date_strings) == 2:
                end_date_str_from_file = date_strings[1]
                parsed_end_date = datetime.strptime(end_date_str_from_file, "%Y%m%d").date()
                if latest_merged_end_date is None or parsed_end_date > latest_merged_end_date:
                    latest_merged_end_date = parsed_end_date
            else:
                print(f"Warning: Merged filename {merged_file.name} for symbol {symbol} not in expected format SYMBOL_STARTDATE_ENDDATE.csv after prefix. Dates part: {dates_section}")
        except ValueError:
            print(f"Warning: Could not parse date from merged file {merged_file.name} for symbol {symbol}.")
        except Exception as e_parse:
            print(f"Warning: Error parsing merged file {merged_file.name} for symbol {symbol}: {e_parse}")
    if latest_merged_end_date:
        calculated_new_start_date = latest_merged_end_date + timedelta(days=1)
        effective_start_date_obj_for_symbol = max(start_date_obj, calculated_new_start_date)
        if effective_start_date_obj_for_symbol > end_date_obj:
            print(f"Symbol {symbol}: Adjusted start date {effective_start_date_obj_for_symbol} is after overall end date {end_date_obj}. No new monthly data will be downloaded for this symbol.")
        else:
            print(f"Symbol {symbol}: Found existing merged data ending on {latest_merged_end_date}. Adjusted download start date to: {effective_start_date_obj_for_symbol}")

    for interval in intervals:
      for year in years:
        for month in months:
          current_date = convert_to_date_object('{}-{}-01'.format(year, month))
          if current_date >= effective_start_date_obj_for_symbol and current_date <= end_date_obj:
            path_segment = get_path(trading_type, "klines", "monthly", symbol, interval)
            file_name = "{}-{}-{}-{}.zip".format(symbol.upper(), interval, year, '{:02d}'.format(month))
            full_url = get_download_url(f"{path_segment}{file_name}")
            destination_path = get_destination_dir(os.path.join(path_segment, file_name), folder)
            all_requests_metadata.append({'url': full_url, 'method': 'GET', 'params': {'destination_path': destination_path, 'symbol': symbol, 'interval': interval}})
            if checksum == 1:
              checksum_file_name = f"{file_name}.CHECKSUM"
              checksum_full_url = get_download_url(f"{path_segment}{checksum_file_name}")
              checksum_destination_path = get_destination_dir(os.path.join(path_segment, checksum_file_name), folder)
              all_requests_metadata.append({'url': checksum_full_url, 'method': 'GET', 'params': {'destination_path': checksum_destination_path, 'symbol': symbol, 'interval': interval, 'is_checksum': True}})
    current += 1

  if all_requests_metadata:
    print(f"Preparing to download {len(all_requests_metadata)} monthly files (chunk size {CHUNK_SIZE})...")
    for i in range(0, len(all_requests_metadata), CHUNK_SIZE):
        chunk_metadata = all_requests_metadata[i:i + CHUNK_SIZE]
        chunk_urls = [req['url'] for req in chunk_metadata]
        
        print(f"Processing monthly batch of {len(chunk_urls)} requests (chunk {i//CHUNK_SIZE + 1}/{ (len(all_requests_metadata) + CHUNK_SIZE -1) // CHUNK_SIZE })")
        responses = fgrequests.build(chunk_urls, max_retries=2) # Reverted to max_retries=2

        for j, response in enumerate(responses):
            req_details = chunk_metadata[j]
            if response and response.ok:
                if not req_details['params'].get('is_checksum', False):
                     save_response_content(response, req_details['params']['destination_path'], req_details['params']['symbol'], req_details['params']['interval'], extracted_files_map_for_function)
                else:
                    Path(req_details['params']['destination_path']).parent.mkdir(parents=True, exist_ok=True)
                    with open(req_details['params']['destination_path'], 'wb') as f:
                        f.write(response.content)
                    print(f"Saved checksum: {req_details['params']['destination_path']}")
            else:
                status_code = response.status_code if response else "N/A"
                error_message = "Unknown error"
                if response:
                    try:
                        error_message = response.reason if hasattr(response, 'reason') and response.reason else f"Status {response.status_code}"
                    except Exception:
                        error_message = f"Status {response.status_code}, reason attribute error"
                else:
                    error_message = "No response or connection error"
                print(f"Failed to download {req_details['url']} (HTTP {status_code} - {error_message}). Not re-queueing.")
  else:
    print("No monthly files to download based on the criteria.")
  return extracted_files_map_for_function


def download_daily_klines(trading_type, symbols, num_symbols, intervals, dates, start_date, end_date, folder, checksum):
  current = 0
  if folder is None:
    folder = DEFAULT_OUTPUT_FOLDER
  Path(folder).mkdir(parents=True, exist_ok=True)
  
  extracted_files_map_for_function = {}

  if not start_date:
    start_date_obj = START_DATE
  else:
    start_date_obj = convert_to_date_object(start_date)

  if not end_date:
    end_date_obj = END_DATE
  else:
    end_date_obj = convert_to_date_object(end_date)

  valid_intervals = list(set(intervals) & set(DAILY_INTERVALS))
  print(f"Found {num_symbols} symbols for daily download with intervals: {valid_intervals}")

  all_requests_metadata = [] # Changed name from all_requests

  for symbol in symbols:
    print(f"[{current+1}/{num_symbols}] - Preparing daily {symbol} klines for download")
    effective_start_date_obj_for_symbol = start_date_obj
    output_folder_path = Path(folder)
    merged_file_pattern = f"{symbol.upper()}_*_*.csv"
    latest_merged_end_date = None
    for merged_file in output_folder_path.glob(merged_file_pattern):
        try:
            symbol_prefix_in_filename = symbol.upper() + "_"
            if not merged_file.stem.startswith(symbol_prefix_in_filename):
                continue
            dates_section = merged_file.stem[len(symbol_prefix_in_filename):]
            date_strings = dates_section.split('_')
            if len(date_strings) == 2:
                end_date_str_from_file = date_strings[1]
                parsed_end_date = datetime.strptime(end_date_str_from_file, "%Y%m%d").date()
                if latest_merged_end_date is None or parsed_end_date > latest_merged_end_date:
                    latest_merged_end_date = parsed_end_date
            else:
                print(f"Warning: Merged filename {merged_file.name} for symbol {symbol} not in expected format SYMBOL_STARTDATE_ENDDATE.csv after prefix. Dates part: {dates_section}")
        except ValueError:
            print(f"Warning: Could not parse date from merged file {merged_file.name} for symbol {symbol}.")
        except Exception as e_parse:
            print(f"Warning: Error parsing merged file {merged_file.name} for symbol {symbol}: {e_parse}")
    if latest_merged_end_date:
        calculated_new_start_date = latest_merged_end_date + timedelta(days=1)
        effective_start_date_obj_for_symbol = max(start_date_obj, calculated_new_start_date)
        if effective_start_date_obj_for_symbol > end_date_obj:
            print(f"Symbol {symbol}: Adjusted start date {effective_start_date_obj_for_symbol} is after overall end date {end_date_obj}. No new daily data will be downloaded for this symbol.")
        else:
            print(f"Symbol {symbol}: Found existing merged data ending on {latest_merged_end_date}. Adjusted download start date to: {effective_start_date_obj_for_symbol}")

    for interval in valid_intervals:
      for date_str in dates:
        current_date = convert_to_date_object(date_str)
        if current_date >= effective_start_date_obj_for_symbol and current_date <= end_date_obj:
          path_segment = get_path(trading_type, "klines", "daily", symbol, interval)
          file_name = "{}-{}-{}.zip".format(symbol.upper(), interval, date_str)
          full_url = get_download_url(f"{path_segment}{file_name}")
          destination_path = get_destination_dir(os.path.join(path_segment, file_name), folder)
          all_requests_metadata.append({'url': full_url, 'method': 'GET', 'params': {'destination_path': destination_path, 'symbol': symbol, 'interval': interval}})
          if checksum == 1:
            checksum_file_name = f"{file_name}.CHECKSUM"
            checksum_full_url = get_download_url(f"{path_segment}{checksum_file_name}")
            checksum_destination_path = get_destination_dir(os.path.join(path_segment, checksum_file_name), folder)
            all_requests_metadata.append({'url': checksum_full_url, 'method': 'GET', 'params': {'destination_path': checksum_destination_path, 'symbol': symbol, 'interval': interval, 'is_checksum': True}})
    current += 1
  
  if all_requests_metadata:
    print(f"Preparing to download {len(all_requests_metadata)} daily files (chunk size {CHUNK_SIZE})...")
    for i in range(0, len(all_requests_metadata), CHUNK_SIZE):
        chunk_metadata = all_requests_metadata[i:i + CHUNK_SIZE]
        chunk_urls = [req['url'] for req in chunk_metadata]

        print(f"Processing daily batch of {len(chunk_urls)} requests (chunk {i//CHUNK_SIZE + 1}/{ (len(all_requests_metadata) + CHUNK_SIZE -1) // CHUNK_SIZE })")
        responses = fgrequests.build(chunk_urls, max_retries=2) # Reverted to max_retries=2

        for j, response in enumerate(responses):
            req_details = chunk_metadata[j]
            if response and response.ok:
                if not req_details['params'].get('is_checksum', False):
                    save_response_content(response, req_details['params']['destination_path'], req_details['params']['symbol'], req_details['params']['interval'], extracted_files_map_for_function)
                else:
                    Path(req_details['params']['destination_path']).parent.mkdir(parents=True, exist_ok=True)
                    with open(req_details['params']['destination_path'], 'wb') as f:
                        f.write(response.content)
                    print(f"Saved checksum: {req_details['params']['destination_path']}")
            else:
                status_code = response.status_code if response else "N/A"
                error_message = "Unknown error"
                if response:
                    try:
                        error_message = response.reason if hasattr(response, 'reason') and response.reason else f"Status {response.status_code}"
                    except Exception:
                         error_message = f"Status {response.status_code}, reason attribute error"
                else:
                    error_message = "No response or connection error"
                print(f"Failed to download {req_details['url']} (HTTP {status_code} - {error_message}). Not re-queueing.")
  else:
    print("No daily files to download based on the criteria.")
  return extracted_files_map_for_function


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
            elif len(date_str_parts) == 2 : # YYYY-MM, assume last day of month for sorting recency
                 # For monthly, use last day of month to compare recency against daily files
                next_month = date(year, month, 1).replace(day=28) + timedelta(days=4)
                return next_month - timedelta(days=next_month.day)
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
        next_month = date(year, month, 1).replace(day=28) + timedelta(days=4)
        return next_month - timedelta(days=next_month.day)
        
    print(f"Warning: Could not parse date from filename: {filename}. Using epoch for sorting.")
    return date(1970, 1, 1)


def merge_symbol_klines_csvs(symbol, csv_file_paths, output_directory):
    if not csv_file_paths:
        print(f"No CSV files provided for symbol {symbol}. Skipping merge.")
        return

    all_dfs = []
    canonical_header = CANONICAL_KLINES_HEADERS # Directly assign the canonical headers
    # Removed logic for determining canonical_header from files (lines 314-332)

    for f_path_str in csv_file_paths:
        f_path = Path(f_path_str)
        if not f_path.exists():
            print(f"Warning: File not found {f_path_str}, skipping.")
            continue
        try:
            df = pd.read_csv(f_path, header=None)
            
            # Fix timestamps: check if first and 7th columns have 16-digit values and convert to 13-digit
            if not df.empty and len(df.columns) >= 7:
                try:
                    # Check both columns 0 (open_time) and 6 (close_time) for 16-digit timestamps
                    first_col = df.iloc[:, 0]
                    seventh_col = df.iloc[:, 6]
                    
                    # Convert to string to check length, regardless of original data type
                    sample_first = str(first_col.dropna().iloc[0]) if len(first_col.dropna()) > 0 else ""
                    sample_seventh = str(seventh_col.dropna().iloc[0]) if len(seventh_col.dropna()) > 0 else ""
                    
                    # Check if timestamps are 16 digits (need to be reduced to 13)
                    if (len(sample_first) == 16 and sample_first.isdigit()) or (len(sample_seventh) == 16 and sample_seventh.isdigit()):
                        print(f"Fixing 16-digit timestamps in {f_path_str} by removing last 3 digits")
                        
                        # Fix first column (open_time) - remove last 3 digits if 16 digits long
                        df.iloc[:, 0] = first_col.apply(lambda x: str(int(x))[:-3] if len(str(int(x))) == 16 else str(int(x)))
                        
                        # Fix seventh column (close_time) - remove last 3 digits if 16 digits long
                        df.iloc[:, 6] = seventh_col.apply(lambda x: str(int(x))[:-3] if len(str(int(x))) == 16 else str(int(x)))
                        
                        print(f"Fixed timestamps: {sample_first} -> {str(int(first_col.iloc[0]))[:-3] if len(str(int(first_col.iloc[0]))) == 16 else str(int(first_col.iloc[0]))}")
                        
                except Exception as e:
                    print(f"Warning: Error during timestamp fix for {f_path_str}: {e}")
            
            df.columns = CANONICAL_KLINES_HEADERS
            all_dfs.append(df)
        except pd.errors.EmptyDataError:
            print(f"Warning: Empty CSV file encountered: {f_path_str}, skipping.")
            continue
        except Exception as e:
            print(f"Error reading CSV {f_path_str}: {e}, skipping.")
            continue
    
    # The canonical_header is now directly assigned, so the fallback logic is simplified.
    # We still need to ensure all_dfs is not empty before proceeding.
    if not all_dfs:
        print(f"No valid CSV data found to merge for symbol {symbol}.")
        return
    
    processed_dfs = []
    for i, df_to_process in enumerate(all_dfs):
        if df_to_process.empty: # Skip empty dataframes
            print(f"Skipping empty dataframe for {symbol} (file index {i}).")
            continue
        if not df_to_process.columns.equals(pd.Index(canonical_header)):
            print(f"Re-ordering columns for a CSV for {symbol} to match canonical header.")
            df_reordered = pd.DataFrame(columns=canonical_header)
            for col in canonical_header:
                if col in df_to_process.columns:
                    df_reordered[col] = df_to_process[col]
                else:
                    df_reordered[col] = pd.NA 
            processed_dfs.append(df_reordered)
        else:
            processed_dfs.append(df_to_process)
    
    if not processed_dfs:
        print(f"No valid, non-empty CSV data left to merge for symbol {symbol} after header processing.")
        return

    merged_df = pd.concat(processed_dfs, ignore_index=True)

    timestamp_column = merged_df.columns[0]
    
    merged_df[timestamp_column] = pd.to_numeric(merged_df[timestamp_column], errors='coerce')
    merged_df = merged_df.dropna(subset=[timestamp_column]) # Drop rows where timestamp became NaN
    if merged_df.empty:
        print(f"Warning: No valid timestamp data found for symbol {symbol} after numeric conversion. Skipping merge.")
        return
    merged_df[timestamp_column] = pd.to_datetime(merged_df[timestamp_column], unit='ms', errors='coerce')
    
    # Drop rows where timestamp became NaT after datetime conversion
    merged_df = merged_df.dropna(subset=[timestamp_column])
    if merged_df.empty:
        print(f"Warning: Merged DataFrame for {symbol} is empty after timestamp conversion and NaT removal. Skipping save.")
        return

    merged_df.sort_values(by=timestamp_column, inplace=True)
    
    merged_df.drop_duplicates(subset=[timestamp_column], keep='first', inplace=True)

    # Check for empty or all NaT timestamp column before calculating min/max dates
    if merged_df.empty or merged_df[timestamp_column].isnull().all():
        print(f"Warning: Merged DataFrame for {symbol} is empty or timestamp column contains only NaT values. Skipping save.")
        return

    min_date_str = merged_df[timestamp_column].min().strftime('%Y%m%d')
    max_date_str = merged_df[timestamp_column].max().strftime('%Y%m%d')
    
    merged_df[timestamp_column] = merged_df[timestamp_column].astype(np.int64) // 10**6


    output_filename = f"{symbol.upper()}_{min_date_str}_{max_date_str}.csv"
    output_file_path = Path(output_directory) / output_filename
    
    try:
        merged_df.to_csv(output_file_path, index=False)
        print(f"Successfully merged {len(csv_file_paths)} CSVs for {symbol} into: {output_file_path}")
        
    except Exception as e:
        print(f"Error saving merged CSV for {symbol} to {output_file_path}: {e}")


if __name__ == "__main__":
    parser = get_parser('klines')
    args = parser.parse_args(sys.argv[1:])

    if args.folder is None:
        args.folder = DEFAULT_OUTPUT_FOLDER
    Path(args.folder).mkdir(parents=True, exist_ok=True)
    print(f"Using output folder: {args.folder}")

    if not args.symbols:
      print("fetching all symbols from exchange")
      symbols = get_all_symbols(args.type)
      num_symbols = len(symbols)
    else:
      symbols = args.symbols
      num_symbols = len(symbols)

    if args.dates:
      dates = args.dates
    else:
      try:
          from enums import PERIOD_START_DATE
      except ImportError:
          print("Warning: PERIOD_START_DATE not found in enums.py, using default '2017-01-01'.")
          PERIOD_START_DATE = "2017-01-01"
      
      period = convert_to_date_object(datetime.today().strftime('%Y-%m-%d')) - convert_to_date_object(
        PERIOD_START_DATE)
      dates = pd.date_range(end=datetime.today(), periods=period.days + 1).to_pydatetime().tolist()
      dates = [date.strftime("%Y-%m-%d") for date in dates]

    all_extracted_csvs = {}

    if args.skip_monthly == 0:
      monthly_csvs = download_monthly_klines(args.type, symbols, num_symbols, args.intervals, args.years, args.months, args.startDate, args.endDate, args.folder, args.checksum)
      for symbol, paths in monthly_csvs.items():
          if symbol not in all_extracted_csvs:
              all_extracted_csvs[symbol] = []
          all_extracted_csvs[symbol].extend(paths)

    if args.skip_daily == 0:
      daily_csvs = download_daily_klines(args.type, symbols, num_symbols, args.intervals, dates, args.startDate, args.endDate, args.folder, args.checksum)
      for symbol, paths in daily_csvs.items():
          if symbol not in all_extracted_csvs:
              all_extracted_csvs[symbol] = []
          all_extracted_csvs[symbol].extend(paths)

    # Debug: Check what files are available for merging
    print(f"\nDebug: all_extracted_csvs contains: {all_extracted_csvs}")
    
    # Enhanced merging logic: collect both newly extracted files AND existing individual files
    all_symbols_to_merge = set(symbols)  # All symbols we're processing
    
    # Add any symbols that had extracted files
    if all_extracted_csvs:
        all_symbols_to_merge.update(all_extracted_csvs.keys())
    
    print(f"Debug: Symbols to check for merging: {all_symbols_to_merge}")
    
    symbols_with_files_to_merge = {}
    
    for symbol in all_symbols_to_merge:
        symbol_files = []
        
        # Add newly extracted files
        if symbol in all_extracted_csvs:
            symbol_files.extend(all_extracted_csvs[symbol])
            print(f"Debug: {symbol} - Added {len(all_extracted_csvs[symbol])} newly extracted files")
        
        # Also collect any existing individual CSV files that haven't been merged
        # Look in the data directory structure for this symbol
        data_pattern_monthly = f"{args.folder}/data/spot/monthly/klines/{symbol.upper()}/*/{symbol.upper()}-*-*-*.csv"
        data_pattern_daily = f"{args.folder}/data/spot/daily/klines/{symbol.upper()}/*/{symbol.upper()}-*-*-*-*.csv"
        
        import glob
        print(f"Debug: {symbol} - Checking monthly pattern: {data_pattern_monthly}")
        print(f"Debug: {symbol} - Checking daily pattern: {data_pattern_daily}")
        
        existing_monthly_files = glob.glob(data_pattern_monthly)
        existing_daily_files = glob.glob(data_pattern_daily)
        
        print(f"Debug: {symbol} - Monthly files found: {existing_monthly_files}")
        print(f"Debug: {symbol} - Daily files found: {existing_daily_files}")
        
        all_existing_files = existing_monthly_files + existing_daily_files
        print(f"Debug: {symbol} - Found {len(all_existing_files)} existing individual CSV files")
        
        # Filter out files that are already covered by existing merged files
        # Get the latest merged file end date for this symbol
        output_folder_path = Path(args.folder)
        merged_file_pattern = f"{symbol.upper()}_*_*.csv"
        latest_merged_end_date = None
        
        for merged_file in output_folder_path.glob(merged_file_pattern):
            try:
                symbol_prefix_in_filename = symbol.upper() + "_"
                if not merged_file.stem.startswith(symbol_prefix_in_filename):
                    continue
                dates_section = merged_file.stem[len(symbol_prefix_in_filename):]
                date_strings = dates_section.split('_')
                if len(date_strings) == 2:
                    end_date_str_from_file = date_strings[1]
                    parsed_end_date = datetime.strptime(end_date_str_from_file, "%Y%m%d").date()
                    if latest_merged_end_date is None or parsed_end_date > latest_merged_end_date:
                        latest_merged_end_date = parsed_end_date
            except:
                continue
        
        print(f"Debug: {symbol} - Latest merged end date: {latest_merged_end_date}")
        
        # Only include files that are after the latest merged date
        if latest_merged_end_date:
            filtered_files = []
            for file_path in all_existing_files:
                try:
                    file_date = parse_date_from_filename(file_path)
                    if file_date > latest_merged_end_date:
                        filtered_files.append(file_path)
                        print(f"Debug: {symbol} - Including file {file_path} (date: {file_date})")
                    else:
                        print(f"Debug: {symbol} - Skipping file {file_path} (date: {file_date}) - already merged")
                except:
                    print(f"Debug: {symbol} - Could not parse date from {file_path}")
            symbol_files.extend(filtered_files)
        else:
            symbol_files.extend(all_existing_files)
            print(f"Debug: {symbol} - No existing merged file, including all {len(all_existing_files)} individual files")
        
        if symbol_files:
            symbols_with_files_to_merge[symbol] = symbol_files
            print(f"Debug: {symbol} - Total files to merge: {len(symbol_files)}")
    
    if symbols_with_files_to_merge:
        print(f"\nStarting CSV merging process for {len(symbols_with_files_to_merge)} symbols...")
        for symbol, csv_paths in symbols_with_files_to_merge.items():
            print(f"Merging {len(csv_paths)} files for symbol {symbol}")
            merge_symbol_klines_csvs(symbol, csv_paths, args.folder)
    else:
        print("No files found to merge.")