import pandas as pd
import requests
import os
from datetime import date
import json

# --- Configuration ---
# Replace with your actual API key
API_KEY = "8e50b23e5c803b8deca7dd5a" 
# Define local paths for the source data and where to save raw API output
CLICKSTREAM_FILE = 'clickstream.csv'
TRANSACTIONS_FILE = 'transactions.csv'
RAW_API_DIR = f"data/raw/api_currency/{date.today().strftime('%Y-%m-%d')}"
CURRENCY_RATES_FILE = f"{RAW_API_DIR}/currency_rates.json"

# Ensure the output directory for raw API data exists
os.makedirs(RAW_API_DIR, exist_ok=True)

# =======================================================
# 1. Function to read CSV files in chunks (Task 2 - Part A)
# =======================================================

def read_csv_in_chunks(filepath, chunk_size=50000):
    """
    Reads a large CSV file in specified chunks and yields each chunk (DataFrame).
    """
    print(f"-> Starting to read {filepath} in chunks of {chunk_size} rows...")
    try:
        # Use pandas.read_csv with the chunksize parameter
        chunks = pd.read_csv(filepath, chunksize=chunk_size)
        for i, chunk in enumerate(chunks):
            print(f"   Yielding chunk {i+1} with {len(chunk)} rows.")
            # Yield the chunk for further processing (e.g., transformation, loading)
            yield chunk
    except FileNotFoundError:
        print(f"Error: The file {filepath} was not found.")
        # As per Task 5 (Logging & alerts), print a warning if inputs are missing
    except Exception as e:
        print(f"An error occurred while reading the CSV: {e}")


# =======================================================
# 2. Function to fetch currency rates via API (Task 2 - Part B)
# =======================================================

def fetch_currency_rates(api_key):
    """
    Fetches the latest USD-based currency conversion rates from the API.
    """
    url = f"https://v6.exchangerate-api.com/v6/{api_key}/latest/USD"
    print(f"\n-> Fetching currency rates from API: {url}")
    
    try:
        response = requests.get(url)
        response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
        data = response.json()

        if data.get("result") == "success":
            print(f"   Successfully fetched rates for base currency {data['base_code']}.")
            # Store the raw JSON response into the specified path (Task 4 requirement snippet)
            with open(CURRENCY_RATES_FILE, 'w') as f:
                json.dump(data, f, indent=4)
            print(f"   Raw API response saved to {CURRENCY_RATES_FILE}")
            
            return data["conversion_rates"]
        else:
            print(f"API Error: Result was not 'success'. Details: {data.get('error-type')}")
            # As per Task 5 (Logging & alerts), print a warning if API fails
            return None

    except requests.exceptions.RequestException as e:
        print(f"Network or API request failed: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Failed to decode JSON response: {e}")
        return None


# =======================================================
# 3. Execution Example
# =======================================================

if __name__ == '__main__':
    
    # --- Task 2 Part A: Read CSVs in chunks ---
    
    print("-" * 40)
    # Process clickstream data in chunks
    for chunk in read_csv_in_chunks(CLICKSTREAM_FILE, chunk_size=50000):
        # In a real pipeline, you would apply basic transforms (Task 3) here
        pass 
        
    print("-" * 40)
    # Process transactions data in chunks
    for chunk in read_csv_in_chunks(TRANSACTIONS_FILE, chunk_size=50000):
         # In a real pipeline, you would apply basic transforms (Task 3) here
        pass 

    # --- Task 2 Part B: Fetch Currency Rates ---
    
    print("-" * 40)
    # Fetch the currency rates and store them
    rates = fetch_currency_rates(API_KEY)
    
    if rates:
        print("\nAvailable conversion rates (snippet):")
        print(f"  USD to INR: {rates.get('INR')}")
        print(f"  USD to EUR: {rates.get('EUR')}")
        # These rates will be used in Task 3 to enrich transactions data
