import pandas as pd
import os
from datetime import date

# Define file paths for the source data
CLICKSTREAM_FILE = 'clickstream.csv'
TRANSACTIONS_FILE = 'transactions.csv'


# =======================================================
# 1. Function to read CSV files in chunks
# =======================================================

def read_csv_in_chunks(filepath, chunk_size=50000):
    """
    Reads a large CSV file in specified chunks and yields each chunk (DataFrame).
    """
    print(f"--- Starting to read {filepath} in chunks of {chunk_size} rows ---")
    try:
        # Use pandas.read_csv with the chunksize parameter to process large files
        chunks = pd.read_csv(filepath, chunksize=chunk_size)
        for i, chunk in enumerate(chunks):
            print(f"-> Yielding chunk {i+1} with {len(chunk)} rows.")
            yield chunk
    except FileNotFoundError:
        print(f"Error: The file {filepath} was not found.")
    except Exception as e:
        print(f"An error occurred while reading the CSV: {e}")

# =======================================================
# 2. Main Execution
# =======================================================

if __name__ == '__main__':
    
    # 1. Read clickstream data in chunks (demonstration)
    print("\n--- Processing clickstream.csv ---")
    for chunk in read_csv_in_chunks(CLICKSTREAM_FILE):
        # This is where your transformation logic for clickstream data would go 
        pass 
        
    # 2. Read transactions data in chunks (demonstration)
    print("\n--- Processing transactions.csv ---")
    for chunk in read_csv_in_chunks(TRANSACTIONS_FILE):
        # This is where your transformation logic for transactions data would go 
        pass
