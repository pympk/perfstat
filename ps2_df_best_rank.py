# region program description (takes 17 seconds for n_dates = -1, a minimum run)
# The program selects specified date and columns: date_end, k12, k11,
# str_iloc_offsets and slice_stop from df_ps1n2_results and save them to
# df_sym. It combines symbols in columns k12 and k11 to a new column k12_k11 in
# df_sym, and collects symbols in k12_k11 to a list of unique symbols and
# counts.
#
# For each symbol in df_sym, the program subsets df_trade_results_ranked by
# values in df_sym columns: str_iloc_offsets and slice_stop. Only
# df_trade_results_ranked column = k12:k12 or k12:k11 are used. The
# highest Pct_Rank_Sum_Ranks in the subset is appended to df_best_rank. A new
# column CAGR_ct*best_pct_rk which is CAGR_count * best_pct_rank is added to
# df_best_rank. For each symbol, only the highest CAGR_ct*best_pct_rk is kept.
# Another column date_delay of NYSE trading dates is added to df_best_rank.
# Date_delay is date_end delayed by days_trade_delay.
#
# df_best_rank is concatenated to the existing file in path_data_dump, and
# save in path_data_dump. The existing file is archived.
#
# v1 add for_loop for dates in dates_nyse, change from concat_df to concat_df_2
# endregion program description

import pandas as pd
import datetime as dt
import numpy as np
import pandas_market_calendars as mcal

import sys
sys.path.append('C:/Users/ping/Google Drive/python/py_files/perfstat/')
# sys.path.append('/content/drive/My Drive/python/py_files/perfstat/')
from collections import Counter
from util import (
    print_symbol_data,
    plot_symbols,
    concat_df_2,
    NYSE_dates,
)


def nyse_day_delta(day_delay, date_start, num_days=30):
    """The function takes date_start (a single date) and delay it by day_delay
    (a column of intergers) and returns an array of delayed NYSE trading dates.

    Args:
        day_delay(pandas.core.series.Series int64): delay in trading days,
        i.e. column of int.
        date_start(str): date in 'yyyy-mm-dd' format
        num_days(int): calendar day increase from date_start

    Return:
        date_end(numpy.ndarray): array of delayed NYSE trading dates
        e.g. ['2020-12-7', '2020-12-08']
    """
    import datetime

    # import pandas_market_calendars as mcal

    # from datetime import datetime as dt

    # convert to datetime object
    dt_start = dt.datetime.strptime(date_start, "%Y-%m-%d")
    dt_end = dt_start + datetime.timedelta(days=num_days)
    start_date = dt_start.strftime("%Y-%m-%d")  # str
    end_date = dt_end.strftime("%Y-%m-%d")  # str
    nyse = mcal.get_calendar("NYSE")
    date_nyse = nyse.schedule(start_date=start_date, end_date=end_date)
    # convert index from datetime to string
    date_index = mcal.date_range(date_nyse, frequency="1D").strftime(
        "%Y-%m-%d"
    )
    date_start_loc = date_index.get_loc(start_date)
    date_end_loc = date_start_loc + day_delay
    date_end = date_index[date_end_loc]

    return date_end


pd.options.mode.chained_assignment = None
pd.set_option("display.width", 400)
pd.set_option("display.max_columns", 11)
pd.set_option("display.max_colwidth", 20)
pd.set_option("display.precision", 3)

# +++ input +++
# ########## Colab_change
path_symbols_data = "C:/Users/ping/Google Drive/stocks/MktCap2b_AUMtop1200/"
# path_symbols_data = '/content/drive/My Drive/stocks/MktCap2b_AUMtop1200/'
# ########## Colab_change
path_data_dump = path_symbols_data + "VSCode_dump/"
path_archive = path_symbols_data + "~archive/"

# start (if n_dates > 0 )or stop (if n_dates < 0) date
# for iteration on dates_nyse
date_pivot = "2021-05-14"
# date_pivot = dt.date.today().strftime("%Y-%m-%d")  # today's date as string

# number of dates to iterate from date_pivot,
# iterate one day back use -1, can't be 0
# use -3 as default to recalculate days when there is a reverse split
# when there is a erroneous price spike, and prices are corrected next day
n_dates = -50

# list of NYSE trading dates ending with pivot date when len(list) is negative
dates_nyse = NYSE_dates(date_pivot=date_pivot, len_list=n_dates)
print(f"dates_nyse: {dates_nyse}\n")

# mySymbols = ["QQQ", "VOO", "VEA", "VWO", "VUG", "VCIT", "GLD", "BND", "BNDX"]
# mySymbols = ["QQQ", "DNKN", "NVCR", "SFIX", "MRNA"]
# mySymbols = ["QQQ", "ACH", "FTCH", "NVCR", "SFIX", "MRNA", "GLDM"]
# mySymbols = ["QQQ", "SPY"]
# mySymbols = ["VGT", "WCLD", "SMH", "PDBC", "COMT", "BCI", "VWO", "ICLN"]
# mySymbols = ['FTEC', 'WCLD', 'SMH', 'PDBC', 'IEMG', 'ICLN']  # symbols to plot
mySymbols = []

rows_OHLCV_display = 6  # number of rows of OHLCV to display
print_syms_data = True
# print_syms_data = False
# plot_syms = True
plot_syms = False
# verbose = True
verbose = False

filename_df = "df_ps1n2_results"
filename_df_ranked = "df_trade_results_ranked"
date_now = dt.date.today().strftime("%Y-%m-%d")
file_df = path_data_dump + "df_best_rank"
file_df_archive = path_archive + "df_best_rank" + date_now
# +++ end input +++

mySort = [
    "[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]",
    "[2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24]",
    "[3, 6, 9, 12, 15, 18, 21, 24, 27, 30, 33, 36]",
    "[4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48]",
    "[5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60]",
    "[6, 12, 18, 24, 30, 36, 42, 48, 54, 60, 66, 72]",
    "[7, 14, 21, 28, 35, 42, 49, 56, 63, 70, 77, 84]",
    "[8, 16, 24, 32, 40, 48, 56, 64, 72, 80, 88, 96]",
    "[9, 18, 27, 36, 45, 54, 63, 72, 81, 90, 99, 108]",
    "[10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120]",
]
df_org = pd.read_pickle(path_data_dump + filename_df)
print(f"df_org.columns:\n{df_org.columns}\n")
for my_date in dates_nyse:
    my_columns0 = ["date_end", "k12", "k11", "str_iloc_offsets", "slice_stop"]
    df = df_org[my_columns0].sort_values(["date_end"], ascending=False)
    df = df.loc[df["date_end"] == my_date]
    # df = df.loc[(df['date_end'] == my_date) &
    #             (df['slice_stop'] == my_slice_stop)]

    # sort df to order in mySort
    df["str_iloc_offsets"] = pd.Categorical(
        df.str_iloc_offsets, categories=mySort
    )
    df = df.sort_values(by=["str_iloc_offsets", "slice_stop"])
    print(f"\n{filename_df}(len={len(df)})\n{df}\n")

    # combine columns k12 and k11, ignore SettingWithCopyWarning
    df["k12_k11"] = df["k12"] + df["k11"]
    my_columns1 = ["date_end", "k12_k11", "str_iloc_offsets", "slice_stop"]
    df = df[my_columns1]
    print(f"df(len={len(df)}):\n{df}\n")

    # gether unique symbols and counts
    symbols = []
    for row in df.itertuples(index=False):
        for symbol in row.k12_k11:
            symbols.append(symbol)
    counter = Counter(symbols).most_common()
    count_total = len(symbols)
    # unique_symbols = []
    # for symbol, count in counter:
    #     unique_symbols.append(symbol)
    unique_symbols = [symbol for symbol, count in counter]

    # keep rows that have symbols in k12_k11
    df_sym = df[
        pd.DataFrame(df.k12_k11.tolist()).isin(unique_symbols).any(1).values
    ]

    if not df_sym.empty:
        print(f"df_sym(len={len(df_sym)}):\n{df_sym}\n")

        print(f"{my_date} unique {my_columns1[1]} symbol: {unique_symbols}\n")
        myFormat0 = "{:<10s} {:<6s} {:<5s} {:<11s}"
        myFormat1 = "{:<10s} {:<6s} {:<5d} {:<11.3f}"
        print(myFormat0.format("date", "symbol", "count", "count/total"))
        for symbol, count in counter:
            # print(symbol, count)
            print(
                myFormat1.format(my_date, symbol, count, count / count_total)
            )
        print(f"\n{my_date}\n{unique_symbols}\n")
        print("=" * 100, "\n")
        df = pd.read_pickle(path_data_dump + filename_df_ranked)
        df = df.sort_values(by=["Pct_Rank_Sum_Ranks"], ascending=False)
        print(
            f"{filename_df_ranked}.columns(len={len(df.columns)}):\n"
            f"{df.columns}\n"
        )

        my_columns2 = [
            "file",
            "columns",
            "str_iloc_offsets",
            "slice_stop",
            "days_trade_delay",
            "pct_bid_ask_spread",
            "drop_top_pct_CAGR",
            "drop_bottom_pct_CAGR",
        ]
        my_columns3 = [
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
        my_columns4 = [
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
        my_columns5 = [
            "Rank_CAGR_mean/std",
            "Rank_win_mean/std",
            "Sum_Ranks",
            "Pct_Rank_Sum_Ranks",
        ]

        pd.set_option("display.width", 300)
        pd.set_option("display.max_columns", 12)
        pd.set_option("display.max_colwidth", 20)
        print(f"{filename_df_ranked}(len={len(df)}):")
        print(df[my_columns2].head(), "\n")
        print(df[my_columns3].head(), "\n")
        print(df[my_columns4].head(), "\n")
        print(df[my_columns5].head(), "\n" * 2)

        myFormat2 = (
            "{:<10s} {:>6s} {:>52s} {:>10s} {:>16s} {:>20s} {:>10s} {:>15s}"
        )
        myFormat3 = "{:<10s} {:>6s} {:>52s} {:>10d} {:>16d} {:>20s} {:>10.1f} {:>15.3f}"  # NOQA
        cols = [
            "date_end",
            "symbol",
            "str_iloc_offsets",
            "slice_stop",
            "days_trade_delay",
            "date_buy_older_limit",
            "CAGR_count",
            "best_pct_rank",
        ]

        lst_dict = []  # initialize dict for df_best_rank
        # MUST BE sorted by=["Pct_Rank_Sum_Ranks"], ascending=False
        df = df.sort_values(by=["Pct_Rank_Sum_Ranks"], ascending=False)
        for row in df_sym.itertuples(index=False):
            # subset df_trade_results_ranked by row values in df_sym
            for symbol in row.k12_k11:
                df_temp = df.loc[
                    (df["columns"] == "k12:k11")
                    & (df["str_iloc_offsets"] == row.str_iloc_offsets)
                    & (df["slice_stop"] == row.slice_stop)
                ]
                # get row values from the best Pct_Rank_Sum_Ranks from subset
                days_trade_delay = df_temp.iloc[0]["days_trade_delay"]
                date_buy_older_limit = df_temp.iloc[0]["date_buy_older_limit"]
                CAGR_count = df_temp.iloc[0]["CAGR_count"]
                best_pct_rank = df_temp.iloc[0]["Pct_Rank_Sum_Ranks"]
                # put results in lst_dict
                lst_dict.append(
                    {
                        "date_end": row.date_end,
                        "symbol": symbol,
                        "str_iloc_offsets": row.str_iloc_offsets,
                        "slice_stop": row.slice_stop,
                        "days_trade_delay": days_trade_delay,
                        "date_buy_older_limit": date_buy_older_limit,
                        "CAGR_count": CAGR_count,
                        "best_pct_rank": best_pct_rank,
                    }
                )

        df_best_rank = pd.DataFrame(
            lst_dict, columns=cols
        )  # create df from lst_dict
        print(f"df_best_rank(len={len(df_best_rank)}):\n{df_best_rank}\n")
        # add new column that combines CAGR_count and best_pct_rank
        df_best_rank["CAGR_ct*best_pct_rk"] = (
            df_best_rank["CAGR_count"] * df_best_rank["best_pct_rank"]
        )
        df_best_rank = df_best_rank.sort_values(
            by=["symbol", "CAGR_ct*best_pct_rk"], ascending=[True, False]
        )
        print(f"df_best_rank (len={len(df_best_rank)}):\n{df_best_rank}\n")
        # keep the highest CAGR_ct*best_pct_rk row, drop other rows
        df_best_rank = df_best_rank.drop_duplicates(
            subset=["date_end", "symbol"], keep="first"
        )
        print(
            f"df_best_rank with highest CAGR_ct*best_pct_rk "
            f"(len={len(df_best_rank)}):\n{df_best_rank}\n"
        )
        # date_delay are NYSE trading dates.
        # It's date_end delayed by days_trade_delay
        date_start = df_best_rank.iloc[0]["date_end"]
        df_best_rank["date_delay"] = np.vectorize(nyse_day_delta)(
            df_best_rank["days_trade_delay"], date_start
        )
        # list of symbol's count in the order of symbol column in df_best_rank
        sym_freq = [Counter(symbols)[symbol] for symbol in df_best_rank.symbol]

        df_best_rank["sym_freq"] = sym_freq
        cols_df = [
            "date_end",
            "symbol",
            "sym_freq",
            "str_iloc_offsets",
            "slice_stop",
            "days_trade_delay",
            "date_buy_older_limit",
            "CAGR_count",
            "best_pct_rank",
            "CAGR_ct*best_pct_rk",
            "date_delay",
        ]
        df_best_rank = df_best_rank[cols_df]
        pd.set_option("display.max_columns", 10)
        pd.set_option("display.max_colwidth", 15)
        print(
            f"df_best_rank added date_delay (len={len(df_best_rank)}):\n"
            f"{df_best_rank}\n\n"
        )

        if print_syms_data:
            print_symbol_data(
                symbols=(unique_symbols + mySymbols),
                path_symbols_data=path_symbols_data,
                date_end_limit=my_date,
                rows_OHLCV_display=rows_OHLCV_display,
            )
        if plot_syms:
            plot_symbols(
                symbols=(unique_symbols + mySymbols),
                path_data_dump=path_data_dump,
                date_start_limit=None,
                date_end_limit=my_date,
                iloc_offset=252,
                # iloc_offset=504,
            )
        print(f"unique_symbols:\n{unique_symbols}\n")

        # loads file_df and drops rows in column "date_end" where its values
        # equal to my_date.  Then, it concatenate dateframes df and file_df,
        # pickles df, archives file_df, and returns concatenated dataframe
        # df_concat.
        my_column = "date_end"
        df_concat = concat_df_2(
            df_best_rank,
            my_column,
            my_date,
            file_df,
            file_df_archive,
            verbose=verbose,
        )
        pd.set_option("display.width", 400)
        pd.set_option("display.max_columns", 10)
        pd.set_option("display.max_colwidth", 15)
        pd.set_option("display.precision", 3)
        print(f"df_best_rank concatendate with {file_df}:\n{df_concat}\n")

        print(myFormat0.format("date", "symbol", "count", "count/total"))
        for symbol, count in counter:
            print(
                myFormat1.format(my_date, symbol, count, count / count_total)
            )
        print(f"\n{my_date}\n{unique_symbols}\n")
