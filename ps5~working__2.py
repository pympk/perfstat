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
pd.set_option("display.max_columns", 12)
pd.set_option("display.max_colwidth", 20)

path_symbols_data = "C:/Users/ping/Google Drive/stocks/MktCap2b_AUMtop1200/"
path_data_dump = path_symbols_data + "VSCode_dump/"
file_df_best_rank = path_data_dump + "df_best_rank"

# needed for function price_close
df_symbols_close = pd.read_pickle(path_data_dump + "df_symbols_close")
idx_syms = df_symbols_close.columns
idx_dates = df_symbols_close.index.strftime("%Y-%m-%d")

df_best_rank = pd.read_pickle(file_df_best_rank)

# array of constant days-delay
delay_zeros = [0] * len(df_best_rank["date_delay"])
delay_ones = [1] * len(df_best_rank["date_delay"])
delay_twos = [2] * len(df_best_rank["date_delay"])

# +++ add delayed close columns to df_best_rank
# trade signal on date_end, buy at close price on date_end + 1
df_best_rank["close_date_end+1"] = [
    price_close(x, y, z)
    for x, y, z in zip(
        df_best_rank["date_end"], df_best_rank["symbol"], delay_ones
    )
]
# trade signal on date_end, sell at close price on date_end + 2
df_best_rank["close_date_end+2"] = [
    price_close(x, y, z)
    for x, y, z in zip(
        df_best_rank["date_end"], df_best_rank["symbol"], delay_twos
    )
]
# trade signal on date_delay, buy at close price on date_delay
df_best_rank["close_date_delay"] = [
    price_close(x, y, z)
    for x, y, z in zip(
        df_best_rank["date_delay"], df_best_rank["symbol"], delay_zeros
    )
]
# trade signal on date_delay, sell at close price on date_delay + 1
df_best_rank["close_date_delay+1"] = [
    price_close(x, y, z)
    for x, y, z in zip(
        df_best_rank["date_delay"], df_best_rank["symbol"], delay_ones
    )
]

print(f"df_best_rank.columns:\n{df_best_rank.columns}\n")
print(f"df_best_rank:\n{df_best_rank}\n")
print(
    f"df_best_rank.days_trade_delay.describe(): "
    f"{df_best_rank.days_trade_delay.describe()}\n"
)

print(f"len(df_best_rank) before dropna: {len(df_best_rank)}")
df_best_rank = df_best_rank.dropna()
print(f"len(df_best_rank) after dropna: {len(df_best_rank)}")

# subset criteria for sym_freq
min_sym_freq = 1  # 1 seems to be the best ratio_risk_adj_rtn_delay_to_QQQ
df_mask = df_best_rank["sym_freq"] >= min_sym_freq
df_best_rank = df_best_rank[df_mask]
print(
    f"len(df_best_rank) after drop sym_freq >= {min_sym_freq}: "
    f"{len(df_best_rank)}\n"
)
df_best_rank = df_best_rank.sort_values(by=["date_end"])
# print(df_best_rank)
print(f"df_best_rank sorted by date_end:\n{df_best_rank}\n")

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

print(f"df_best_rank:\n{df_best_rank[cols1]}\n")
print(f"df_best_rank:\n{df_best_rank[cols2]}\n")

# add return columns based on signal on date_end and date_delay
df_best_rank["rtn_date_end"] = (
    df_best_rank["close_date_end+2"] / df_best_rank["close_date_end+1"] - 1
)
df_best_rank["rtn_date_delay"] = (
    df_best_rank["close_date_delay+1"] / df_best_rank["close_date_delay"] - 1
)
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
print(f"df_best_rank:\n{df_best_rank[cols1]}\n")
print(f"df_best_rank:\n{df_best_rank[cols3]}\n")

cols4 = [
    "date_end",
    "symbol",
    "rtn_date_end",
    "rtn_date_delay",
]

# df to calculate Ulcer-Index
df_tmp = df_best_rank[cols4]
print(f"df_tmp:\n{df_tmp}\n")
df_best_rank_groupby_date_end = df_best_rank.groupby(["date_end"])
# select only required columns
df_best_rank_groupby_date_end = df_best_rank_groupby_date_end[
    ["date_end", "rtn_date_end", "rtn_date_delay"]
]
# get the average
df_tmp_mean = df_best_rank_groupby_date_end.mean()
print(f"df_tmp_mean:\n{df_tmp_mean}\n")
print(f"df_tmp_mean.columns:\n{df_tmp_mean.columns}\n")
print(f"df_tmp_mean.index:\n{df_tmp_mean.index}\n")
print(f"df_tmp_mean.index[0]:\n{df_tmp_mean.index[0]}\n")
print(f"df_tmp_mean.index[-1]:\n{df_tmp_mean.index[-1]}\n")
date_mean_start = df_tmp_mean.index[0]
date_mean_end = df_tmp_mean.index[-1]

symbols_OHLCV = "dfs_symbols_OHLCV_download"  # OHLCV data for all symbols
# dfs_OHLCV = pickle_load(path_data_dump, symbols_OHLCV)  # load OHLCV data
dfs_OHLCV = pd.read_pickle(path_data_dump + symbols_OHLCV)  # load OHLCV data
df_QQQ = dfs_OHLCV["QQQ"]  # OHLCV for symbols
df_QQQ = df_QQQ.loc[date_mean_start:date_mean_end][["close"]]
print(f"df_QQQ:\n{df_QQQ}\ntype(df_QQQ): {type(df_QQQ)}")
idx_QQQ = df_QQQ.index
# print(idx_QQQ, type(idx_QQQ))
print(f"idx_QQQ:\n{idx_QQQ}\ntype(idx_QQQ): {type(idx_QQQ)}\n")

df_tmp_mean.index.name = None
df_QQQ.index.name = None
# print("\n" * 2)
print(f"type(df_tmp_mean.index): {type(df_tmp_mean.index)}\n")
df_tmp_mean.index = pd.to_datetime(df_tmp_mean.index)
print(f"type(df_tmp_mean.index): {type(df_tmp_mean.index)}\n")
df_tmp_mean_reindex = df_tmp_mean.reindex(idx_QQQ, fill_value=0)
print(f"df_tmp_mean_reindex:\n{df_tmp_mean_reindex}\n")

df_best_rank_group_by_date = df_tmp_mean_reindex
df_best_rank_group_by_date["rtn_end+1"] = (
    1 + df_best_rank_group_by_date["rtn_date_end"]
)
df_best_rank_group_by_date["rtn_delay+1"] = (
    1 + df_best_rank_group_by_date["rtn_date_delay"]
)
# df_best_rank_group_by_date.loc['2015-01-07', 'rtn_end+1'] = 1
# df_best_rank_group_by_date.loc['2015-01-07', 'rtn_delay+1'] = 1
df_best_rank_group_by_date["rtn_end+1"].iloc[0] = 1
df_best_rank_group_by_date["rtn_delay+1"].iloc[0] = 1
print(f"df_best_rank_group_by_date:\n{df_best_rank_group_by_date}\n")


####################################
df_best_rank_group_by_date["close_end"] = 0
df_best_rank_group_by_date["close_delay"] = 0
df_best_rank_group_by_date["close_end"][0] = 1
df_best_rank_group_by_date["close_delay"][0] = 1
print(f"df_best_rank_group_by_date:\n{df_best_rank_group_by_date}\n")
for i in range(1, len(df_best_rank_group_by_date)):
    df_best_rank_group_by_date["close_end"].iloc[i] = (
        df_best_rank_group_by_date["rtn_end+1"].iloc[i]
        * df_best_rank_group_by_date["close_end"].iloc[i - 1]
    )
    df_best_rank_group_by_date["close_delay"].iloc[i] = (
        df_best_rank_group_by_date["rtn_delay+1"].iloc[i]
        * df_best_rank_group_by_date["close_delay"].iloc[i - 1]
    )
print(f"df_best_rank_group_by_date:\n{df_best_rank_group_by_date}\n")
df_best_rank_group_by_date["close_QQQ"] = df_QQQ["close"] / df_QQQ["close"][0]
print(f"df_best_rank_group_by_date:\n{df_best_rank_group_by_date}\n")
print(
    f"ulcer_index(close_end):\n{ulcer_index(df_best_rank_group_by_date.close_end)}\n"
)
print(
    f"ulcer_index(close_delay):\n{ulcer_index(df_best_rank_group_by_date.close_delay)}\n"
)
print(
    f"ulcer_index(close_QQQ):\n{ulcer_index(df_best_rank_group_by_date.close_QQQ)}\n"
)

_, UI_close_end, _ = ulcer_index(df_best_rank_group_by_date.close_end)
_, UI_close_delay, _ = ulcer_index(df_best_rank_group_by_date.close_delay)
_, UI_close_QQQ, _ = ulcer_index(df_best_rank_group_by_date.close_QQQ)


close_end = df_best_rank_group_by_date["close_end"].iloc[-1]
close_delay = df_best_rank_group_by_date["close_delay"].iloc[-1]
close_QQQ = df_best_rank_group_by_date["close_QQQ"].iloc[-1]
print(f"close_end: {close_end:.4f}")
print(f"close_delay: {close_delay:.4f}")
print(f"close_QQQ: {close_QQQ:.4f}\n")


print(f"UI_close_end: {UI_close_end:.4f}")
print(f"UI_close_delay: {UI_close_delay:.4f}")
print(f"UI_close_QQQ: {UI_close_QQQ:.4f}\n")
risk_adj_rtn_close_end = (
    df_best_rank_group_by_date["close_end"].iloc[-1] / UI_close_end
)
risk_adj_rtn_close_delay = (
    df_best_rank_group_by_date["close_delay"].iloc[-1] / UI_close_delay
)
risk_adj_rtn_close_QQQ = (
    df_best_rank_group_by_date["close_QQQ"].iloc[-1] / UI_close_QQQ
)
print(f"risk_adj_rtn_close_end: {risk_adj_rtn_close_end:.4f}")
print(f"risk_adj_rtn_close_delay: {risk_adj_rtn_close_delay:.4f}")
print(f"risk_adj_rtn_close_QQQ: {risk_adj_rtn_close_QQQ:.4f}\n")
ratio_risk_adj_rtn_end_to_QQQ = risk_adj_rtn_close_end / risk_adj_rtn_close_QQQ
ratio_risk_adj_rtn_delay_to_QQQ = (
    risk_adj_rtn_close_delay / risk_adj_rtn_close_QQQ
)
print(f"ratio_risk_adj_rtn_end_to_QQQ: {ratio_risk_adj_rtn_end_to_QQQ:.4f}")
print(
    f"ratio_risk_adj_rtn_delay_to_QQQ: {ratio_risk_adj_rtn_delay_to_QQQ:.4f}\n"
)
ratio_rtn_close_end_to_QQQ = (
    df_best_rank_group_by_date["close_end"].iloc[-1]
    / df_best_rank_group_by_date["close_QQQ"].iloc[-1]
)
ratio_rtn_close_delay_to_QQQ = (
    df_best_rank_group_by_date["close_delay"].iloc[-1]
    / df_best_rank_group_by_date["close_QQQ"].iloc[-1]
)
print(f"ratio_rtn_close_end_to_QQQ: {ratio_rtn_close_end_to_QQQ:.4f}")
print(f"ratio_rtn_close_delay_to_QQQ: {ratio_rtn_close_delay_to_QQQ:.4f}\n")


# df_best_rank_group_by_date.to_csv(path_data_dump + 'working.csv')
