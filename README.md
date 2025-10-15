# DataEngineering
Data Engineering Internship


--- Clickstream Info ---
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 200000 entries, 0 to 199999
Data columns (total 6 columns):
 #   Column      Non-Null Count   Dtype
---  ------      --------------   -----
 0   user_id     200000 non-null  int64
 1   session_id  200000 non-null  object
 2   page_url    200000 non-null  object
 3   click_time  200000 non-null  object
 4   device      200000 non-null  object
 5   location    200000 non-null  object
dtypes: int64(1), object(5)
memory usage: 9.2+ MB

--- Transactions Describe ---
                                      txn_id        user_id  ...  currency              txn_time
count                                 100000  100000.000000  ...    100000                100000     
unique                                100000            NaN  ...         5                 82976     
top     00e22e71-ccc3-4cd8-8b9c-dcd45831360e            NaN  ...       JPY  2025-09-04T19:21:56Z     
freq                                       1            NaN  ...     20178                     6     
mean                                     NaN   24971.033910  ...       NaN                   NaN     
std                                      NaN   14477.990669  ...       NaN                   NaN     
min                                      NaN       1.000000  ...       NaN                   NaN     
25%                                      NaN   12387.750000  ...       NaN                   NaN     
50%                                      NaN   24931.500000  ...       NaN                   NaN     
75%                                      NaN   37503.000000  ...       NaN                   NaN     
max                                      NaN   50000.000000  ...       NaN                   NaN     

[11 rows x 5 columns]
Clickstream duplicates: 
------------------------------------------------------------------------------------------------------------------------------------------
--- Processing clickstream.csv ---
--- Starting to read clickstream.csv in chunks of 50000 rows ---
-> Yielding chunk 1 with 50000 rows.
-> Yielding chunk 2 with 50000 rows.
-> Yielding chunk 3 with 50000 rows.
-> Yielding chunk 4 with 50000 rows.

--- Processing transactions.csv ---
--- Starting to read transactions.csv in chunks of 50000 rows ---
-> Yielding chunk 1 with 50000 rows.
-> Yielding chunk 2 with 50000 rows.
--------------------------------------------------------------------------------------------------------------------------------------


