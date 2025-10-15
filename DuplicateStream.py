import pandas as pd
df_click = pd.read_csv('clickstream.csv')
df_trans = pd.read_csv('transactions.csv')

print("--- Clickstream Info ---")
df_click.info()
print("\n--- Transactions Describe ---")
print(df_trans.describe(include='all'))
print(f"Clickstream duplicates: {df_click.duplicated().sum()}")