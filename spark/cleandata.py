import pandas as pd

df = pd.read_csv("CDPH_Environmental_Inspections_20250425.csv")

df.columns = ["inspection_id", "inspection_name", "address", "street_number_from", "street_number_to", "direction", "street_name", "street_type", "inspection_category", "inspection_sub_category", "inspector", "inspection_date", "result", "result_date", "data_source", "latitude", "longitude", "location"]


df["inspection_date"] = pd.to_datetime(df["inspection_date"], errors="coerce")
df["result_date"] = pd.to_datetime(df["result_date"], errors="coerce")
new_df = df[(df["inspection_date"].dt.year > 2010) | (df["result_date"].dt.year > 2010)]


new_df.to_csv("cdph_environment.csv")