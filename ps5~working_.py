import pandas as pd
from ulcer_index import ulcer_index


def price_close(myDate, mySymbol, myDelay=0, verbose=False):
    """The function returns mySymbol's close price on date = myDate + myDelay.
    df_symbols_close, idx_syms and idx_dates must be present for function
    to work.

    Args:
        myDate(str): date in format 'yyyy-mm-dd'
        mySymbol(str): symbol, e.g. 'QQQ'
        myDelay(int): number of NYSE trading dates delayed from myDate

    Return:
        close(float): mySymbol's closing price on date = myDate + myDelay
          if available, otherwise retruns NaN
    """

    if myDate in idx_dates and mySymbol in idx_syms:
        idx_sym = idx_syms.get_loc(mySymbol)
        idx_date = idx_dates.get_loc(myDate)
        idx_myDate = idx_date + myDelay
        if idx_myDate < len(idx_dates):
            myDate_delay = idx_dates[idx_myDate]
            close = df_symbols_close.iloc[idx_myDate, idx_sym]
            if verbose:
                print(f"{mySymbol} closed at {close } on {myDate_delay}")
        else:
            close = float("NaN")
            myDate_delay = float("NaN")
            if verbose:
                print(f"{mySymbol} closed at {close } on {myDate_delay}")
    else:
        close = float("NaN")
        myDate_delay = float("NaN")
        if verbose:
            print(f"{mySymbol} closed at {close } on {myDate_delay}")

    return close


pd.set_option("display.width", 400)
pd.set_option("display.max_columns", 11)
pd.set_option("display.max_colwidth", 30)

path_symbols_data = "C:/Users/ping/Google Drive/stocks/MktCap2b_AUMtop1200/"
path_data_dump = path_symbols_data + "VSCode_dump/"
file_df = path_data_dump + "df_best_rank"

df_symbols_close = pd.read_pickle(path_data_dump + "df_symbols_close")
idx_syms = df_symbols_close.columns
idx_dates = df_symbols_close.index.strftime("%Y-%m-%d")

df = pd.read_pickle(file_df)

delay_zeros = [0] * len(df["date_delay"])
delay_ones = [1] * len(df["date_delay"])
delay_twos = [2] * len(df["date_delay"])

df["close_date_end+1"] = [
    price_close(x, y, z)
    for x, y, z in zip(df["date_end"], df["symbol"], delay_ones)
]
df["close_date_end+2"] = [
    price_close(x, y, z)
    for x, y, z in zip(df["date_end"], df["symbol"], delay_twos)
]
df["close_date_delay"] = [
    price_close(x, y, z)
    for x, y, z in zip(df["date_delay"], df["symbol"], delay_zeros)
]
df["close_date_delay+1"] = [
    price_close(x, y, z)
    for x, y, z in zip(df["date_delay"], df["symbol"], delay_ones)
]
print(df)
print(f"len(df) before dropna: {len(df)}")
df = df.dropna()
print(f"len(df) after dropna: {len(df)}")

min_sym_freq = 1
df_mask = df["sym_freq"] >= min_sym_freq
df = df[df_mask]
print(f"len(df) after drop sym_freq >= {min_sym_freq}: {len(df)}")
df = df.sort_values(by=["date_end"])
print(df)

cols1 = [
    "date_end",
    "symbol",
    "sym_freq",
    "str_iloc_offsets",
    "slice_stop",
    "days_trade_delay",
    "date_buy_older_limit",
    "CAGR_count",
    "best_pct_rank",
]
cols2 = [
    "date_end",
    "symbol",
    "CAGR_ct*best_pct_rk",
    "date_delay",
    "close_date_end+1",
    "close_date_end+2",
    "close_date_delay",
    "close_date_delay+1",
]

print(f"df:\n{df[cols1]}\n")
print(f"df:\n{df[cols2]}\n")

df["rtn_date_end"] = df["close_date_end+2"] / df["close_date_end+1"] - 1
df["rtn_date_delay"] = df["close_date_delay+1"] / df["close_date_delay"] - 1
cols3 = [
    "date_end",
    "symbol",
    "CAGR_ct*best_pct_rk",
    "date_delay",
    "close_date_end+1",
    "close_date_end+2",
    "close_date_delay",
    "close_date_delay+1",
    "rtn_date_end",
    "rtn_date_delay",
]
print(f"df:\n{df[cols1]}\n")
print(f"df:\n{df[cols3]}\n")

cols4 = [
    "date_end",
    "symbol",
    "rtn_date_end",
    "rtn_date_delay",
]
df_ui = df[cols4]
print(f"df_ui:\n{df_ui}\n")
df_groupby_date_end = df.groupby(["date_end"])
# select only required columns
df_groupby_date_end = df_groupby_date_end[
    ["date_end", "rtn_date_end", "rtn_date_delay"]
]
# get the average
df_ui_mean = df_groupby_date_end.mean()
print(f"df_ui_mean:\n{df_ui_mean}\n")
print(f"df_ui_mean.columns:\n{df_ui_mean.columns}\n")
print(f"df_ui_mean.index:\n{df_ui_mean.index}\n")
print(f"df_ui_mean.index[0]:\n{df_ui_mean.index[0]}\n")
print(f"df_ui_mean.index[-1]:\n{df_ui_mean.index[-1]}\n")
date_mean_start = df_ui_mean.index[0]
date_mean_end = df_ui_mean.index[-1]

symbols_OHLCV = "dfs_symbols_OHLCV_download"  # OHLCV data for all symbols
# dfs_OHLCV = pickle_load(path_data_dump, symbols_OHLCV)  # load OHLCV data
dfs_OHLCV = pd.read_pickle(path_data_dump + symbols_OHLCV)  # load OHLCV data
df_QQQ = dfs_OHLCV["QQQ"]  # OHLCV for symbols
df_QQQ = df_QQQ.loc[date_mean_start:date_mean_end][["close"]]
print(f"df_QQQ:\n{df_QQQ}\ntype(df_QQQ): {type(df_QQQ)}")
idx_QQQ = df_QQQ.index
print(idx_QQQ, type(idx_QQQ))
# subset OHLCV to date_end_limit
# df_QQQ = df_QQQ.loc[:date_end_limit].sort_values(by='date', ascending=False)

df_ui_mean.index.name = None
df_QQQ.index.name = None
print("\n" * 2)
# print(type(df_ui_mean.index))
print(type(idx_QQQ))
df_ui_mean.index = pd.to_datetime(df_ui_mean.index)
print(type(df_ui_mean.index))
df_ui_mean_reindex = df_ui_mean.reindex(idx_QQQ, fill_value=0)
print(f"df_ui_mean_reindex:\n{df_ui_mean_reindex}\n")

df = df_ui_mean_reindex
df["rtn_end+1"] = 1 + df["rtn_date_end"]
df["rtn_delay+1"] = 1 + df["rtn_date_delay"]
# df.loc['2015-01-07', 'rtn_end+1'] = 1
# df.loc['2015-01-07', 'rtn_delay+1'] = 1
df["rtn_end+1"].iloc[0] = 1
df["rtn_delay+1"].iloc[0] = 1
print(f"df:\n{df}\n")

df["close_end"] = 0
df["close_delay"] = 0
df["close_end"][0] = 1
df["close_delay"][0] = 1
print(f"df:\n{df}\n")
for i in range(1, len(df)):
    df["close_end"].iloc[i] = (
        df["rtn_end+1"].iloc[i] * df["close_end"].iloc[i - 1]
    )
    df["close_delay"].iloc[i] = (
        df["rtn_delay+1"].iloc[i] * df["close_delay"].iloc[i - 1]
    )
print(f"df:\n{df}\n")
df["close_QQQ"] = df_QQQ["close"] / df_QQQ["close"][0]
print(f"df:\n{df}\n")
# print(f"ulcer_index(close_end):\n{ulcer_index(df.close_end)}\n")
# print(f"ulcer_index(close_delay):\n{ulcer_index(df.close_delay)}\n")
# print(f"ulcer_index(close_QQQ):\n{ulcer_index(df.close_QQQ)}\n")

_, UI_close_end, _ = ulcer_index(df.close_end)
_, UI_close_delay, _ = ulcer_index(df.close_delay)
_, UI_close_QQQ, _ = ulcer_index(df.close_QQQ)

print(f"UI_close_end: {UI_close_end:.4f}")
print(f"UI_close_delay: {UI_close_delay:.4f}")
print(f"UI_close_QQQ: {UI_close_QQQ:.4f}\n")
adj_rtn_close_end = df["close_end"].iloc[-1] / UI_close_end
adj_rtn_close_delay = df["close_delay"].iloc[-1] / UI_close_delay
adj_rtn_close_QQQ = df["close_QQQ"].iloc[-1] / UI_close_QQQ
print(f"adj_rtn_close_end: {adj_rtn_close_end:.4f}")
print(f"adj_rtn_close_delay: {adj_rtn_close_delay:.4f}")
print(f"adj_rtn_close_QQQ: {adj_rtn_close_QQQ:.4f}\n")
ratio_adj_rtn_end_QQQ = adj_rtn_close_end / adj_rtn_close_QQQ
ratio_adj_rtn_delay_QQQ = adj_rtn_close_delay / adj_rtn_close_QQQ
print(f"ratio_adj_rtn_end_QQQ: {ratio_adj_rtn_end_QQQ:.4f}")
print(f"ratio_adj_rtn_delay_QQQ: {ratio_adj_rtn_delay_QQQ:.4f}\n")

# df.to_csv(path_data_dump + 'working.csv')
