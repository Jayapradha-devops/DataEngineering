import pandas as pd
from google.cloud import storage
from datetime import date
import os

# --- Configuration ---
GCS_BUCKET_NAME = "data_manipulation_bucket" # Replace with your actual bucket name
INGEST_DATE_PARTITION = f"ingest_date={date.today().strftime('%Y-%m-%d')}"
STORAGE_CLIENT = storage.Client()

def upload_dataframe_to_gcs_partitioned(df_chunk: pd.DataFrame, dataset_name: str, chunk_index: int):
    """
    Writes a pandas DataFrame chunk to GCS with a Hive-style partition structure.
    
    The final path will look like:
    gs://your-unique-bucket-name/cleaned/{dataset_name}/ingest_date=YYYY-MM-DD/part_{chunk_index}.parquet
    """
    
    # 1. Define the destination path within GCS
    # Using Parquet format is generally best practice for cloud storage and BigQuery
    destination_blob_name = f"cleaned/{dataset_name}/{INGEST_DATE_PARTITION}/part_{chunk_index}.parquet"
    
    # 2. Convert DataFrame to a local temporary Parquet file
    local_temp_file = f"temp_chunk_{dataset_name}_{chunk_index}.parquet"
    df_chunk.to_parquet(local_temp_file, index=False)

    # 3. Upload the temporary file to GCS
    try:
        bucket = STORAGE_CLIENT.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(local_temp_file)
        
        print(f"Successfully uploaded chunk {chunk_index} of {dataset_name} to:")
        print(f"gs://{GCS_BUCKET_NAME}/{destination_blob_name}")
        
    except Exception as e:
        print(f"Failed to upload to GCS: {e}")

    finally:
        # 4. Clean up the local temporary file
        if os.path.exists(local_temp_file):
            os.remove(local_temp_file)


# =======================================================
# Example Usage within a chunking loop
# =======================================================

if __name__ == '__main__':
    # This example creates a dummy dataframe to simulate a "cleaned" chunk
    data = {'col1': [1, 2, 3], 'col2': ['A', 'B', 'C'], 'timestamp_utc': [pd.Timestamp.utcnow()] * 3}
    cleaned_chunk_df = pd.DataFrame(data)
    
    # Assuming this was the first chunk processed
    chunk_id = 1 
    dataset = 'transactions' # or 'clickstream'

    upload_dataframe_to_gcs_partitioned(cleaned_chunk_df, dataset, chunk_id)
