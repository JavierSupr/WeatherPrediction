import pandas as pd

file1 = "file1.csv"
file2 = "file2.csv" 

df1 = pd.read_csv(file1)
df2 = pd.read_csv(file2)

if 'Datetime' in df1.columns and 'Tavg' in df1.columns and 'Datetime' in df2.columns and 'Tavg' in df2.columns:

    merged_df = pd.merge(df1, df2, on=['Datetime', 'Tavg'], how='outer')

    merged_df.to_csv("merged_data.csv", index=False)
    print("Data berhasil digabungkan dan disimpan dalam 'merged_data.csv'.")
else:
    print("Kolom 'Datetime' dan 'Tavg' tidak ditemukan di salah satu file CSV.")
