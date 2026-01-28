import sys
import pandas as pd
import pyarrow
print("Program arguments", sys.argv)

month = int(sys.argv[1])
print(f"Running pipeline for month {month}")

df = pd.DataFrame({"Month": [month, month], "Day": [1, 2], "Passangers": [3, 4]})
print(df.head())

df.to_parquet(f"output_month_{sys.argv[1]}.parquet")