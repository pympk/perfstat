import pandas as pd
import sys
sys.path.append("C:/Users/ping/Google Drive/python/py_files/perfstat/")
# sys.path.append('/content/drive/My Drive/python/py_files/perfstat/')
from util import pickle_load, pickle_dump


# +++ input +++
path_symbols_data = "C:/Users/ping/Google Drive/stocks/MktCap2b_AUMtop1200/"
path_data_dump = path_symbols_data + "VSCode_dump/"
filename_df_in = "df_trade_results"
filename_df_out = "df_trade_results_ranked"
# +++ end input +++

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 10)
pd.set_option("display.max_colwidth", 15)

df = pickle_load(path_data_dump, filename_df_in)
print(df.columns, "\n")
print(df.head())

my_columns1 = [
    "file",
    "columns",
    "str_iloc_offsets",
    "slice_stop",
    "days_trade_delay",
    "pct_bid_ask_spread",
    "drop_top_pct_CAGR",
    "drop_bottom_pct_CAGR",
]
my_columns2 = [
    "date_buy_older_limit",
    "date_buy_newer_limit",
    "CAGR_count",
    "CAGR_min",
    "CAGR_25%",
    "CAGR_50%",
    "CAGR_75%",
    "CAGR_max",
    "CAGR_mean",
]
my_columns3 = [
    "CAGR_std",
    "CAGR_mean/std",
    "win_mean",
    "win_std",
    "win_mean/std",
    "trades/week",
    "days_held_mean",
    "days_held_std",
    "days_held_mean/std",
]
df = df.sort_values(by=["CAGR_mean/std"], ascending=False)
print(df[my_columns1].head(), "\n")
print(df[my_columns2].head(), "\n")
print(df[my_columns3].head(), "\n" * 2)
print(df[my_columns1].tail(), "\n")
print(df[my_columns2].tail(), "\n")
print(df[my_columns3].tail())
print(f"df.date_buy_older_limit.unique(): {df.date_buy_older_limit.unique()}")
print(f"len(df) before drop rows: {len(df)}")

# Get indexes where name column has value john, faster than query
index_drop = df[df["CAGR_count"] < 0].index
# Delete these row indexes from dataFrame
df.drop(index_drop, inplace=True)
df = df.dropna(subset=["CAGR_mean/std"])
print(f"len(df) after drop rows: {len(df)}")

df["Rank_CAGR_mean/std"] = df["CAGR_mean/std"].rank()
df["Rank_win_mean/std"] = df["win_mean/std"].rank()
df["Sum_Ranks"] = df["Rank_CAGR_mean/std"] + df["Rank_win_mean/std"]
df["Pct_Rank_Sum_Ranks"] = df["Sum_Ranks"].rank(pct=True)
df = df.sort_values(by=["Pct_Rank_Sum_Ranks"], ascending=False)
my_columns4 = [
    "Rank_CAGR_mean/std",
    "Rank_win_mean/std",
    "Sum_Ranks",
    "Pct_Rank_Sum_Ranks",
]
pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 9)
pd.set_option("display.max_colwidth", 15)
print(len(df))
print(df[my_columns1].head(), "\n")
print(df[my_columns2].head(), "\n")
print(df[my_columns3].head(), "\n")
print(df[my_columns4].head(), "\n" * 2)
print(df[my_columns1].tail(), "\n")
print(df[my_columns2].tail(), "\n")
print(df[my_columns3].tail(), "\n")
print(df[my_columns4].tail())
pickle_dump(df, path_data_dump, filename_df_out)
