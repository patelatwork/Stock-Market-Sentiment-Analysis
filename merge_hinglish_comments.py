import pandas as pd
import glob
import os
import re
from pathlib import Path

def extract_ticker_from_filename(filename):
    """Extract ticker symbol from filename"""
    # Extract ticker from filename pattern like 'BHARTIARTL_hinglish_comments_only_...'
    basename = os.path.basename(filename)
    ticker = basename.split('_')[0]
    return ticker

def merge_hinglish_csv_files(data_folder="Data(NLP)"):
    """Merge all hinglish comment CSV files into a single file"""
    
    # Find all hinglish comment CSV files
    pattern = os.path.join(data_folder, "*_hinglish_comments_only_*.csv")
    csv_files = glob.glob(pattern)
    
    if not csv_files:
        print(f"No hinglish comment CSV files found in {data_folder}")
        return
    
    print(f"Found {len(csv_files)} CSV files to merge")
    
    merged_data = []
    
    for file_path in csv_files:
        try:
            # Extract ticker from filename
            ticker_symbol = extract_ticker_from_filename(file_path)
            
            # Read CSV file
            df = pd.read_csv(file_path)
            
            # Select required columns (using exact column names from the CSV)
            required_columns = ['Post_Title', 'Post_Date', 'Comment_Text', 'Combined_Sentiment']
            
            # Check if all required columns exist
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                print(f"Warning: Missing columns in {file_path}: {missing_columns}")
                # Print available columns for debugging
                print(f"Available columns: {list(df.columns)}")
                continue
            
            # Select and rename columns
            selected_df = df[required_columns].copy()
            
            # Add ticker symbol column
            selected_df['Ticker_Symbol'] = ticker_symbol
            
            # Add source file for reference
            selected_df['Source_File'] = os.path.basename(file_path)
            
            merged_data.append(selected_df)
            
            print(f"Processed {file_path}: {len(selected_df)} records")
            
        except Exception as e:
            print(f"Error processing {file_path}: {str(e)}")
            continue
    
    if not merged_data:
        print("No data to merge")
        return
    
    # Combine all dataframes
    final_df = pd.concat(merged_data, ignore_index=True)
    
    # Reorder columns
    column_order = ['Ticker_Symbol', 'Post_Title', 'Post_Date', 'Comment_Text', 'Combined_Sentiment', 'Source_File']
    final_df = final_df[column_order]
    
    # Save merged data
    output_filename = "merged_hinglish_comments.csv"
    final_df.to_csv(output_filename, index=False, encoding='utf-8')
    
    print(f"\nMerging complete!")
    print(f"Total records: {len(final_df)}")
    print(f"Output file: {output_filename}")
    print(f"Unique tickers: {final_df['Ticker_Symbol'].nunique()}")
    
    # Display summary
    ticker_counts = final_df['Ticker_Symbol'].value_counts()
    print(f"\nRecords per ticker:")
    print(ticker_counts)
    
    # Display sample data
    print(f"\nSample of merged data:")
    print(final_df.head(3))
    
    return final_df

if __name__ == "__main__":
    # Change to the correct directory
    os.chdir(r"d:\Programs\NLP PROJECT")
    
    # Run the merge function
    merged_df = merge_hinglish_csv_files()