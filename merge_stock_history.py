import pandas as pd
import os
from glob import glob
import re

# Define the data directory
data_dir = "Data(NLP)"

# Find all stock history files (CSV only, excluding XLS files)
stock_history_files = glob(os.path.join(data_dir, "*_stock_history_*.csv"))

print(f"Found {len(stock_history_files)} stock history CSV files")

# List to store individual dataframes
dfs = []

# Process each file
for file in stock_history_files:
    print(f"Processing: {os.path.basename(file)}")
    
    # Extract ticker symbol from filename (e.g., "RELIANCE" from "RELIANCE_stock_history_...")
    filename = os.path.basename(file)
    ticker_match = re.match(r'^([A-Z]+)_stock_history', filename)
    
    if ticker_match:
        ticker = ticker_match.group(1)
        
        try:
            # Read the CSV file
            df = pd.read_csv(file)
            
            # Add ticker symbol column
            df['Ticker_Symbol'] = ticker
            
            # Append to list
            dfs.append(df)
            print(f"  - Loaded {len(df)} rows for {ticker}")
            
        except Exception as e:
            print(f"  - Error reading {file}: {e}")
    else:
        print(f"  - Could not extract ticker symbol from filename")

# Combine all dataframes
if dfs:
    print("\nCombining all stock history data...")
    merged_data = pd.concat(dfs, ignore_index=True)
    
    print(f"Total rows before removing duplicates: {len(merged_data)}")
    
    # Convert Date column to datetime for proper handling
    merged_data['Date'] = pd.to_datetime(merged_data['Date'])
    
    # Remove duplicates based on Date and Ticker_Symbol (keep first occurrence)
    merged_data = merged_data.drop_duplicates(subset=['Date', 'Ticker_Symbol'], keep='first')
    
    print(f"Total rows after removing duplicates: {len(merged_data)}")
    
    # Sort by Ticker_Symbol and Date
    merged_data = merged_data.sort_values(['Ticker_Symbol', 'Date']).reset_index(drop=True)
    
    # Save the merged data
    output_file = "merged_stock_history.csv"
    merged_data.to_csv(output_file, index=False)
    
    print(f"\nMerged stock history saved to: {output_file}")
    print(f"\nSummary by Ticker:")
    print(merged_data.groupby('Ticker_Symbol').size())
    
    print(f"\nDate range:")
    print(f"Earliest date: {merged_data['Date'].min()}")
    print(f"Latest date: {merged_data['Date'].max()}")
    
    print(f"\nFirst few rows:")
    print(merged_data.head())
    
    print(f"\nColumns: {list(merged_data.columns)}")
    
else:
    print("No stock history files found to merge!")
