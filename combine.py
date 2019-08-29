import pandas as pd
import sys

file_1 = sys.argv[1]
file_2 = "end_time.csv"


df_1 = pd.read_csv(file_1)
df_2 = pd.read_csv(file_2)
df_1= df_1.merge(df_2, on=["job"],how = "left")
df = df_1.sort_values(["start_time"])
df["completed_time"] = df["end_time"]-df["init_time"]-df["start_time"]
df["end_time"] = df["start_time"]+df["completed_time"]
df = df.drop(["init_time"], axis=1)
df.to_csv("output.csv")
