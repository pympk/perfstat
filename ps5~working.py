import pandas as pd


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

# myDate = '2018-12-27'
# mySymbol = 'QQQ'
# myDelay = -1
# close = price_close(myDate, mySymbol, myDelay, verbose=True)
# print(f"mySymbol: {mySymbol}, myDate: {myDate}, myDelay: {myDelay}, "
#       f"close: {close}")
# myDelay = 0
# close = price_close(myDate, mySymbol, myDelay, verbose=True)
# print(f"mySymbol: {mySymbol}, myDate: {myDate}, myDelay: {myDelay}, "
#       f"close: {close}")
# myDelay = 1
# close = price_close(myDate, mySymbol, myDelay, verbose=True)
# print(f"mySymbol: {mySymbol}, myDate: {myDate}, myDelay: {myDelay}, "
#       f"close: {close}")


# df = pd.read_pickle(file_df).tail(2)
df = pd.read_pickle(file_df)
# print(df)

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
print(f'df["rtn_date_end"].describe():\n{df["rtn_date_end"].describe()}\n')
print(
    f'df["rtn_date_end"].mean() / df["rtn_date_end"].std():\n'
    f'{df["rtn_date_end"].mean() / df["rtn_date_end"].std()}\n'
)
print(f'df["rtn_date_delay"].describe():\n{df["rtn_date_delay"].describe()}\n')
print(
    f'df["rtn_date_delay"].mean() / df["rtn_date_delay"].std():\n'
    f'{df["rtn_date_delay"].mean() / df["rtn_date_delay"].std()}\n'
)

df_temp = df[['date_end', 'symbol']]
print(df_temp.head(30))