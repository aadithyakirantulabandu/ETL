import pandas as pd, pyarrow
print("pyarrow", pyarrow.__version__)
df = pd.read_parquet(r"masked_out\cleaned.parquet")
print(df.head(10).to_string(index=False))
print("\nrows:", len(df))
