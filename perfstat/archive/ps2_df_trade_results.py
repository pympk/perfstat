# region program description
# The program iterates on the combination of set_file_df_results,
# set_slice_stops, set_iloc_offsets, and set_cols, and consolidate the symbols
# in file_df_results, e.g. 'df_ps1n2_results', in the selected columns,
# (e.g k12, k11, ...k5), into a single column (e.g. k12:k5). It creates a
# previous_row column from the single column (e.g. k12:k5) time shifted to
# by 1 to the previous day. It finds the list difference between these two
# columns (e.g. k12:k5) and previous_row, to create dataframe df_symbols_in_out
# with 3 columns: date, symbols_in, symbols_out.
#
# Symbols_in are symbols in the
# selected columns with symbols from the previous day removed,
# i.e. symbols_in = set(k12:k5) - set(previous_day).
#
# Symbols_out are symbols from the previous date with symbols from the selected
# columns removed, i.e. symbols_out = set(previous_day) - set(k12:k5).
#
# The function Trades takes df_symbols_in_out, with columns: index, date_end,
# symbols_in, symbols_out, and generates df_trades with columns: index,
# symbol, date_in, date_out, loc_date_in, loc_date_out, date_buy, date_sell,
# loc_diff, and days_trade_delay. Date_in is the date the symbol enters
# the symbols_in column in df_symbols_in_out. Date_out is the date symbol
# enters the symbols_out column. loc_date_in and loc_date_out are index
# locations of date_in and date_out. Date_buy and date_sell are date_in and
# date_out delayed by days_trade_delay. Loc_diff is holding period of the
# trade, i.e. loc_date_out - loc_date_in. The last column in df_trades is
# days_trade_delay.
#
# The function Returns takes date_buy/date_sell from df_trades and create
# df_returns with columns: symbol, date_buy, date_sell, price_buy, price_sell
# loc_diff,	CAGR_%, win. Half of the bid-ask-spread is applied to each of the
# buy/sell price. The function calculates CAGR_% and win/lose for each
# trades and append them to df_trades and saves df_trades to
# df_trades_n_returns.
#
# df_returns is a subset of df_trades where a top/bottom percentage of data,
# sorted by CAGR_%, are dropped. df_returns is subsetted where the date_buy is
# within the date_buy_older_limit/date_buy_newer_limit.
#
# The function Returns returns parameters, and trade statistics: CAGR_%,
# win/lose, trade_per_week are returned as dictionary to be appended to
# a dataframe.
#
# Current run results are appended to df_trade_results. Deleteing it deletes
# previous results. Previous runs are archived in ~archive directory.
#
#
# v1 add set_iloc_offsets loop, add data clean to remove symbols that don't
#    have csv data, write run parameters to dict: trade_results
# v2 use itertools to loop through set_iloc_offsets, set_cols,
#    and set_file_df_results
# v3 test for df_returns.empty
# v4 test df_org.query(my_query) returns empty df, if not df_trades.empty:
#    add subset by iloc_offsets and slice_stop
# v5 drop rows with zeros in df_trades column close_date_buy
#    and close_date_sell
# v6 fix days_trade_delay == 0 by if: df = df_org,
#    else: df = df_org.iloc[:-days_trade_delay]
# v7 add set_days_trade_delay, removed 'date_buy_newer_limit'
#    from columns_drop_duplicate
# v8 if (not df.empty) and (len(df) > 1) was if not df.empty
#    test if len(df) >= 2: before check sort from oldest to newest
# endregion program description

import pandas as pd
import json
import numpy as np
import os
import datetime as dt
import itertools
import sys

# ########## Colab_change
sys.path.append("/content/drive/MyDrive/python/py_files/perfstat/")
# ########## Colab_change
from numpy import loadtxt
from util import pickle_load, pickle_dump, list_dump, append_df_2


# region functions
def flatten_list_of_lists(myColumn):
    """The function takes a dataframe with a single column. Each row is a
    list-of-lists of symbols. The function flattens the list-of-list in each
    row and return it as list of symbols.

    Args:
        myColumn(dataframe): dataframe with a single column of list-of-lists,
          e.g.  [[], [], ['NIO'], ['TSLA', 'SE'], ['IRTC', 'W'], []]

    Return:
        list: flattened list of symbols,
         e.g. ['NIO', 'TSLA', 'SE', 'IRTC', 'W']
    """
    # # for loop equivalent to list comprehension
    # flattened_list = []
    # for myRow in myColumn:
    #     for myList in myRow:
    #         for symbol in myList:
    #             flattened_list.append(symbol)
    # return flattened_list
    return [
        symbol for myRow in myColumn for myList in myRow for symbol in myList
    ]


def list_diff(x):
    """The function takes a dataframe with two columns, x[0], x[1]. Each column
    has a list of symbols in each row.  The function returns two lists:
    symbols_in and symbols_out. Symbols_in is a list of symbols in x[0] that
    are not in x[1]. Symbols_out is a list of symbols in x[1] that are not in
    x[0].

    Args:
        x(dataframe): dataframe with two columns, x[0], x[1]. Each column has a
         list of symbols in each row.

    Return:
        symbols_in(list): list of symbols in x[0] that are not in x[1]
        symbols_out(list): list of symbols in x[1] that are not in x[0]
    """

    symbols_in = list(set(x[0]) - set(x[1]))
    symbols_out = list(set(x[1]) - set(x[0]))
    return symbols_in, symbols_out


def trades(days_trade_delay=1, verbose=False):
    """The program takes df_symbols_in_out, with columns: index, date_end,
    symbols_in, symbols_out, and generates df_trades with columns: index,
    symbol, date_in, date_out, loc_date_in, loc_date_out, date_buy, date_sell,
    and loc_diff. Date_in is the date the symbol enters symbols_in in
    df_symbols_in_out. Date_out is the date symbol enters symbols_out.
    loc_date_in and loc_date_out are index locations of date_in and date_out.
    Date_buy and date_sell are date_in and date_out delayed by
    days_trade_delay. Loc_diff is holding period of the trade,
    i.e. loc_date_out - loc_date_in. The last column in df_trades is
    days_trade_delay.

    Args:
        days_trade_delay(int): delay in days between buy/sell signals from
         df_symbols_in_out to actural buy/sell.

    Return: None
    """

    def match_symbols_in_dicts(
        l_dicts, l_symbols, date_out, trades, myKey="symbol"
    ):
        """l_symbols is a list of symbols, e.g. ['A', 'AAPL', ... , 'Z'].
        l_dicts is a list of dictionaries with date_in, e.g. [{'symbol':'A',
        'date_in':'2020-01-01'}, ... , {'symbol':'Z', 'date_in':'2020-02-02'}]
        If a symbol in l_symbols is also a symbol in l_dicts, the function
        removes the symbol's dictionary from l_dicts,
        adds 'date_out':'yyyy-mm-dd' to the dictionary, and appends the
        dictionary to trades.  After going through the symbols in l_symbols,
        the function returns the updated l_dicts and trades.

        Args:
            l_dicts: list of dictionaries with keys: 'symbol' and 'date_in',
            e.g. [{'symbol':'A','date_in':'2020-01-01'},
            ... , {'symbol':'Z', 'date_in':'2020-02-02'}]
            l_symbols: list of symbols, e.g. ['A', ... , 'Z']
            date_out(str): 'yyyy-mm-dd'
            trades: list of dicts with keys: 'symbol', 'date_in', 'date_out',
            use to create dataframe.
            e.g. [{'symbol':'A','date_in':'2020-01-01', 'date_out':'2020-01-02'},  # NOQA
            .. , {'symbol':'Z', 'date_in':'2020-02-02', 'date_out':'2020-02-03'}]

        Return:
            l_dicts: list of dictionaries with keys: 'symbol' and 'date_in',
            e.g. [{'symbol':'A','date_in':'2020-01-01'},
            ... , {'symbol':'Z', 'date_in':'2020-02-02'}]
            trades: list of dicts with keys: 'symbol', 'date_in', 'date_out'
            e.g. [{'symbol':'A','date_in':'2020-01-01', 'date_out':'2020-01-02'},
            .. , {'symbol':'Z', 'date_in':'2020-02-02', 'date_out':'2020-02-03'}]
        """
        # e.g. l_symbols = ['A', 'AAPL', ... , 'FRPT']
        for myValue in l_symbols:
            # e.g. l_dicts = [{'symbol':'A', 'date_in':'2020-01-01'}, ...
            #   {'symbol':'B', 'date_in':'2020-02-02'}]
            #  iterate on dicts in l_dicts, i is dict's position in l_dicts
            #  use l_dicts.copy() to not modify the list in for-loop
            for i, dict in enumerate(l_dicts.copy()):
                for key, value in dict.items():
                    # symbol in l_symbols is in l_dicts
                    if myKey == key and myValue == value:
                        dict_out = l_dicts[i].copy()
                        del l_dicts[i]  # remove dict with this symbol
                        # add date_out date to dict
                        dict_out.update({"date_out": date_out})
                        # write dict with date_out date
                        trades.append(dict_out)

        return l_dicts, trades

    pd.set_option("display.max_colwidth", 80)
    pd.set_option("display.width", 550)
    pd.set_option("display.max_columns", 9)

    # path_symbols_data = \
    #     'C:/Users/ping/Google Drive/stocks/MktCap2b_AUMtop1200/'  # NOQA
    # path_data_dump = path_symbols_data + 'VSCode_dump/'
    # days_trade_delay = 1  # delay between trade signal and actural trade
    df_org = pickle_load(path_data_dump, "df_symbols_in_out")
    if verbose:
        print("+" * 20 + " trades function start " + "+" * 20)
        print("df_org:\n{}\n".format(df_org))

    # replace index with column 'date_end'
    df_org.set_index("date_end", drop=True, inplace=True)
    df_org.index.names = ["date"]  # rename index from 'date_end' to 'date'
    # sort date oldest to newest
    df_org.sort_index(ascending=True, inplace=True)
    date_index = df_org.index
    if verbose:
        print("df_org, sorted oldest to newest:\n{}\n".format(df_org))

    # dropped last rows (rows with the newest date) to compensate for
    #  trade delays to prevent:
    #  IndexError: index 6031 is out of bounds for axis 0 with size 6031
    if days_trade_delay == 0:
        df = df_org
    else:
        df = df_org.iloc[:-days_trade_delay]

    # +++ error check: raise errors if date is not sorted
    #  from oldest to newest +++
    if len(df) >= 2:
        if df.index[0] > df.index[1]:
            msg1 = f"df.index[0]: {df.index[0]} +++ SHOULD BE OLDER THAN +++ df.index[1]: {df.index[1]}\n"  # NOQA
            msg2 = "column 'date' may not be sorted with ascending=True"
            err_msg = msg1 + msg2
            raise ValueError(err_msg)

    # +++ create df_trades, columns: symbol, date_in, date_out ++++
    sym_net_in = []  # list-of_dicts with keys: symbol, date_in
    trades = []  # list-of_dicts with keys: symbol, date_in, date_out
    # df row with index: date', columns: 'symbols_in', 'symbols_out'

    for date, row in df.iterrows():
        for sym in row[0]:  # first column symbols_in
            sym_net_in.append({"symbol": sym, "date_in": date})
        # if symbol in symbols_out is in sym_net_in
        #  delete symbol from sym_net_in,
        #  add date_out to symbol and append symbol to trade
        sym_net_in, trades = match_symbols_in_dicts(
            sym_net_in, row[1], date_out=date, trades=trades, myKey="symbol"
        )
    # df_trades with columns: index, symbol, date_in, date_out
    df_trades = pd.DataFrame(trades)
    df_trades.to_csv(path_data_dump + "df_trades.csv")
    pickle_dump(df_trades, path_data_dump, "df_trades")
    # print('df_trades:\n{}\n'.format(df_trades))

    # ++++ create df_trades columns: ++++
    #  loc_date_in, loc_date_out, date_buy, date_sell, loc_diff
    loc_date_in = []  # to create loc_date_in column
    loc_date_out = []  # to create loc_date_out column
    date_buy = []  # to create date_buy column
    date_sell = []  # to create date_sell column

    for row in df_trades.itertuples(index=False):
        tmp_loc_in = date_index.get_loc(row.date_in)
        tmp_loc_out = date_index.get_loc(row.date_out)
        loc_date_in.append({"loc_date_in": tmp_loc_in})
        loc_date_out.append({"loc_date_out": tmp_loc_out})
        tmp_date_buy = date_index[tmp_loc_in + days_trade_delay]
        tmp_date_sell = date_index[tmp_loc_out + days_trade_delay]
        date_buy.append({"date_buy": tmp_date_buy})
        date_sell.append({"date_sell": tmp_date_sell})

    df_loc_date_in = pd.DataFrame(loc_date_in, index=df_trades.index)
    df_loc_date_out = pd.DataFrame(loc_date_out, index=df_trades.index)
    df_date_buy = pd.DataFrame(date_buy, index=df_trades.index)
    df_date_sell = pd.DataFrame(date_sell, index=df_trades.index)
    df_trades = pd.concat(
        [
            df_trades,
            df_loc_date_in,
            df_loc_date_out,
            df_date_buy,
            df_date_sell,
        ],
        axis=1,
    )

    # release df from memory
    df_loc_date_in = pd.DataFrame()
    df_loc_date_out = pd.DataFrame()
    df_date_buy = pd.DataFrame()
    df_date_sell = pd.DataFrame()

    if not df_trades.empty:
        df_trades["loc_diff"] = (
            df_trades["loc_date_out"] - df_trades["loc_date_in"]
        )
        df_trades["days_trade_delay"] = days_trade_delay
        if verbose:
            print(f"df_trades:\n{df_trades}\n")
        pickle_dump(df_trades, path_data_dump, "df_trades")
        df_trades.to_csv(path_data_dump + "df_trades.csv")

        # ++++ add days_trade_delay to run parameters, and write to file ++++
        trade_results = pickle_load(path_data_dump, "trade_results")
        # add 'days_trade_delay' to run paramenters dict
        trade_results["days_trade_delay"] = days_trade_delay

        file_tmp = path_data_dump + "trade_results.txt"
        with open(file_tmp, "w") as f:
            # use json.loads to do the reverse
            f.write(json.dumps(trade_results))
        pickle_dump(trade_results, path_data_dump, "trade_results")
        if verbose:
            print(f"trade_results: {trade_results}")
            print("-" * 20 + " trades function end " + "-" * 20)

    return None


def returns(
    pct_bid_ask_spread=0.3,
    drop_top_pct_CAGR=0,
    drop_bottom_pct_CAGR=0,
    date_buy_older_limit=None,
    date_buy_newer_limit=None,
    verbose=False,
):
    """The program adds buy/sell prices from df_symbols_close to trades
    in df_trades. Half of the bid-ask-spread is applied to each of the
    buy/sell price. The program calculates CAGR_% and win/lose for each
    trades and append them to df_trades and saves df_trades to
    df_trades_n_returns.

    df_returns is a subset of df_trades where a top/bottom percentage of data,
    sorted by CAGR_%, are dropped. df_returns is subset of
    df_returns where the date_buy is within the
    date_buy_older_limit/date_buy_newer_limit.

    The function parameters, and trade statistics: CAGR_%, win/lose,
    trade_per_week are returned as dictionary to be appended to a dataframe.

    Args:
        pct_bid_ask_spread(float): percent bid-ask-spread applied to buy/sell
          prices, e.g. 0.3 is 0.3%
        drop_top_pct_CAGR(float): percent of the total rows of the best CAGR_%
          to drop, e.g. 1 is 1 percent
        drop_bottom_pct_CAG: percent of the total rows of the worst CAGR_%
          to drop, e.g. 1 is 1 percent
        date_buy_older_limit(str): older date limit to subset df_returns,
          e.g. start date in form of '2020-01-03'
        date_buy_newer_limit(str): newer date limit to subset df_returns,
          e.g. end date in form of '2020-02-03'

    Return:
        trade_results(dict): dictionary of function parameters and trade
          statistics to be appended to a dataframe where the column names
          are keys and parameters and statistics are values
    """
    pd.set_option("display.max_colwidth", 25)
    pd.set_option("display.width", 550)
    pd.set_option("display.max_columns", 12)

    df_symbols_close = pickle_load(path_data_dump, "df_symbols_close")
    close_date_index = df_symbols_close.index
    close_symbol_index = df_symbols_close.columns

    if verbose:
        print("+" * 20 + " return function start " + "+" * 20)
        print(f"df_symbols_close:\n{df_symbols_close}\n")
        # end='' result in continuation in the same line
        print(f"close_date_index({type(close_date_index)}):\n", end="")
        print(f"{close_date_index}\n")
        print(f"close_symbol_index({type(close_symbol_index)}):\n", end="")
        print(f"{close_symbol_index}")

    df_trades = pickle_load(path_data_dump, "df_trades")

    trade_results = {}
    if not df_trades.empty:
        # list of symbols in df_trades but not in df_symbols_close
        symbols_not_in_df_symbols_close = list(
            set(
                df_trades.symbol[
                    ~df_trades.symbol.isin(close_symbol_index)
                ].tolist()
            )
        )
        if verbose:
            print(
                f"symbols_not_in_df_symbols_close:\n"
                f"{symbols_not_in_df_symbols_close}\n"
            )
        list_dump(
            symbols_not_in_df_symbols_close,
            path_data_dump,
            "symbols_not_in_df_symbols_close.txt",
        )

        # data clean: select symbols in df_trades that are in df_symbols_close
        df_trades = df_trades[df_trades.symbol.isin(close_symbol_index)]
        if verbose:
            print(
                f"df_trades after removing symbols that are not "
                f"in df_symbols_close(len: {len(df_trades)}):\n"
                f"{df_trades}\n"
            )

        # create df_tmp that has the same index as df_trades,
        #  df_tmp has index for date_buy, date_sell, and symbol
        index_buy_sell_symbol = []
        row_index_df_trades = []
        symbols_not_in_df_symbols_close = []

        # faster with intertuples than iterrows
        for row in df_trades.itertuples(index=True):
            loc_date_buy = close_date_index.get_loc(row.date_buy)
            loc_date_sell = close_date_index.get_loc(row.date_sell)
            loc_symbol = close_symbol_index.get_loc(row.symbol)
            # print(f'row:\n{row}\n')
            index_buy_sell_symbol.append(
                {
                    "date_buy_index": loc_date_buy,
                    "date_sell_index": loc_date_sell,
                    "symbol_index": loc_symbol,
                }
            )
            # row_index_df_trades.append(i)
            row_index_df_trades.append(row.Index)

        df_tmp = pd.DataFrame(index_buy_sell_symbol, index=row_index_df_trades)
        if verbose:
            print(f"df_tmp:\n{df_tmp}\n")

        # create df_tmp2 that has the same index as df_trades,
        #  df_tmp2 has closing prices for date_buy and date_sell,
        #  half of bid-ask-spread is applied to both price_buy and price_sell
        price_buy_sell = []

        # intertuples faster than iterrows
        for row in df_tmp.itertuples(index=False):
            close_date_buy = df_symbols_close.iat[
                row.date_buy_index, row.symbol_index
            ]
            close_date_sell = df_symbols_close.iat[
                row.date_sell_index, row.symbol_index
            ]
            price_buy_sell.append(
                {
                    "close_date_buy": close_date_buy,
                    "close_date_sell": close_date_sell,
                }
            )
        df_tmp2 = pd.DataFrame(price_buy_sell, index=df_trades.index)
        if verbose:
            print(f"df_tmp2:\n{df_tmp2}\n")

        # apply bid-ask spread, 0.3 means 0.3% bid-ask-spread
        #  add half of bid-ask spread to each buy and sell
        multi_price_buy = 1 + (pct_bid_ask_spread / 2) / 100
        multi_price_sell = 1 - (pct_bid_ask_spread / 2) / 100
        df_tmp2["multi_price_buy"] = multi_price_buy
        df_tmp2["multi_price_sell"] = multi_price_sell
        df_tmp2["price_buy"] = df_tmp2["close_date_buy"] * multi_price_buy
        df_tmp2["price_sell"] = df_tmp2["close_date_sell"] * multi_price_sell
        if verbose:
            print(f"df_tmp2:\n{df_tmp2}\n")

        # combine dfs and release dfs from memory
        df_trades = pd.concat([df_trades, df_tmp, df_tmp2], axis=1)
        # TODO if close_date_buy or close_date_sell == 0
        # delete symbol from return calculation, add to a log
        df_tmp = pd.DataFrame()
        df_tmp2 = pd.DataFrame()

        pd.set_option("display.max_columns", 14)
        pd.options.display.float_format = "{:,.5}".format
        # calculate compounded-annual-growth-rate
        df_trades["CAGR_%"] = 100 * (
            np.power(
                (df_trades.price_sell / df_trades.price_buy),
                (252 / df_trades.loc_diff),
            )
            - 1
        )
        df_trades["win"] = np.where(df_trades["CAGR_%"] > 0, 1, 0)

        # drop Nan rows in df_trades
        df_tmp3 = df_trades[df_trades.isnull().any(axis=1)]
        df_tmp3 = df_tmp3[
            [
                "symbol",
                "date_buy",
                "date_sell",
                "close_date_buy",
                "close_date_sell",
            ]
        ]

        # df_trades_close_with_zero are df_trades with zeros
        #  in column: ['close_date_buy', 'close_date_sell']
        df_trades_close_with_zero = df_trades[
            (df_trades[["close_date_buy", "close_date_sell"]] == 0).any(axis=1)
        ]
        df_trades_close_with_zero.to_csv(
            path_data_dump + "df_trades_close_with_zero"
        )

        if verbose:
            print("These df_trades rows have Nan and are dropped:")
            print(df_tmp3, "\n")
            print(
                "These rows in df_trades are dropped. They have zero in\n"
                'columns ["close_date_buy", "close_date_sell"]'
            )
            print(f"df_trades_close_with_zero:\n{df_trades_close_with_zero}\n")

        # data clean
        df_trades = df_trades.dropna()
        # drop rows with zeros in df_trades column close_date_buy
        #  and close_date_sell
        df_trades = df_trades[
            ~(df_trades[["close_date_buy", "close_date_sell"]] == 0).any(
                axis=1
            )
        ]

        # release from memory
        df_tmp3 = pd.DataFrame()
        df_trades_close_with_zero = pd.DataFrame()

        # create df_returns from select df_trades columns
        my_columns = [
            "symbol",
            "date_buy",
            "date_sell",
            "price_buy",
            "price_sell",
            "loc_diff",
            "CAGR_%",
            "win",
        ]

        pd.set_option("display.max_colwidth", 15)
        pd.set_option("display.width", 400)
        pd.set_option("display.max_columns", 12)

        df_trades = df_trades.sort_values(by=["date_buy"])
        df_returns = df_trades[my_columns]
        if verbose:
            print(f"df_trades_n_returns sorted by date_buy:\n{df_trades}\n")
        pickle_dump(df_trades, path_data_dump, "df_trades_n_returns")
        df_trades.to_csv(path_data_dump + "df_trades_n_returns.csv")
        df_trades = pd.DataFrame()  # release dfs from memory

        # number of rows, with the best CAGR_%, to drop
        df_returns = df_returns.sort_values(by=["CAGR_%"])
        top_df_row_drop = int(drop_top_pct_CAGR / 100 * len(df_returns))
        # number of rows, with the worst CAGR_%, to drop
        bottom_df_row_drop = int(drop_bottom_pct_CAGR / 100 * len(df_returns))
        start = bottom_df_row_drop
        end = len(df_returns) - top_df_row_drop
        if verbose:
            print(f"bottom_df_row_drop: {bottom_df_row_drop}")
            print(f"top_df_row_drop: {top_df_row_drop}")
            print(f"len(df_returns): {len(df_returns)}")
            print(f"df_returns = df_returns[{start}:{end}]")
            print(
                f"df_returns sorted by CAGR_% before dropping top "
                f"{drop_top_pct_CAGR}% "
                f"and bottom {drop_bottom_pct_CAGR}%"
            )
            print(f"len(df_returns): {len(df_returns)}")
            print(f"df_returns:\n{df_returns}\n")  # must sort by CAGR_%
        # must be sorted by CAGR_%, drops rows with the best and the worst CAGR_%  # NOQA
        # df_returns = df_returns[bottom_df_row_drop:-top_df_row_drop]
        df_returns = df_returns[start:end]
        if verbose:
            print(
                f"df_returns sorted by CAGR_% after dropping top "
                f"{drop_top_pct_CAGR}% ",
                f"and bottom {drop_bottom_pct_CAGR}%",
            )
            print(f"len(df_returns): {len(df_returns)}")
            print(f"df_returns:\n{df_returns}\n")  # must sort by CAGR_%
        df_returns = df_returns.sort_values(by=["date_buy"])
        pickle_dump(df_returns, path_data_dump, "df_returns")
        df_returns.to_csv(path_data_dump + "df_returns.csv")
        if verbose:
            print("df_returns.tail(40) sorted by date_buy:")
            print(df_returns.tail(40), "\n")
            print(f"Wrote df_returns to {path_data_dump}df_returns.csv\n")

        # TODO ++++++++ df_org is already subsetted to date_buy limits ++++++++
        # subset df_returns by limits on date_buy
        df_returns = df_returns.sort_values(by=["date_buy"])
        # if date_buy_older_limit is None:  # must sort by date_buy
        #     date_buy_older_limit = df_returns.iloc[0]['date_buy']
        # if date_buy_newer_limit is None:  # must sort by date_buy
        #     date_buy_newer_limit = df_returns.iloc[-1]['date_buy']
        # start-date is date_buy_older_limit, end-date is date_buy_newer_limit
        # df_returns = \
        #     df_returns[(df_returns['date_buy'] >= date_buy_older_limit) &
        #                (df_returns['date_buy'] <= date_buy_newer_limit)]
        if df_returns.empty:
            trade_results = {}
        else:
            # sort oldest to newest
            df_returns = df_returns.sort_values(by=["date_buy"])
            df_returns_desc = df_returns.describe()
            if verbose:
                print(
                    f"date_buy limits: {date_buy_older_limit} <= date_buy "
                    f"<= {date_buy_newer_limit}\n"
                )
                print(f"df_returns sorted by date_buy:\n" f"{df_returns}\n")
                print(f"df_returns.describe:\n" f"{df_returns_desc}\n")

            # calculate trades per week
            date_first = df_returns.iloc[0]["date_buy"]
            date_last = df_returns.iloc[-1]["date_buy"]
            loc_date_first = close_date_index.get_loc(date_first)
            loc_date_last = close_date_index.get_loc(date_last)
            loc_diff = loc_date_last - loc_date_first
            # case where df_returns has one row
            if loc_diff == 0:
                trades_per_week = 0
            else:
                # 5 trading days/week
                trades_per_week = len(df_returns) / loc_diff * 5

            # ++++ add run parameters, and write to file ++++
            trade_results = pickle_load(path_data_dump, "trade_results")
            trade_results["pct_bid_ask_spread"] = pct_bid_ask_spread
            trade_results["drop_top_pct_CAGR"] = drop_top_pct_CAGR
            trade_results["drop_bottom_pct_CAGR"] = drop_bottom_pct_CAGR
            trade_results["date_buy_older_limit"] = date_buy_older_limit
            trade_results["date_buy_newer_limit"] = date_buy_newer_limit
            trade_results["CAGR_count"] = df_returns_desc["CAGR_%"]["count"]
            trade_results["CAGR_min"] = df_returns_desc["CAGR_%"]["min"]
            trade_results["CAGR_25%"] = df_returns_desc["CAGR_%"]["25%"]
            trade_results["CAGR_50%"] = df_returns_desc["CAGR_%"]["50%"]
            trade_results["CAGR_75%"] = df_returns_desc["CAGR_%"]["75%"]
            trade_results["CAGR_max"] = df_returns_desc["CAGR_%"]["max"]
            trade_results["CAGR_mean"] = df_returns_desc["CAGR_%"]["mean"]
            trade_results["CAGR_std"] = df_returns_desc["CAGR_%"]["std"]
            if df_returns_desc["CAGR_%"]["std"] == 0:
                trade_results["CAGR_mean/std"] = 0
            else:
                trade_results["CAGR_mean/std"] = (
                    df_returns_desc["CAGR_%"]["mean"]
                    / df_returns_desc["CAGR_%"]["std"]
                )
            trade_results["win_mean"] = df_returns_desc["win"]["mean"]
            trade_results["win_std"] = df_returns_desc["win"]["std"]
            # prevent case where mean and std are both zero will get warning:
            #  RuntimeWarning: invalid value encountered in double_scalars
            if df_returns_desc["win"]["std"] == 0:
                trade_results["win_mean/std"] = 0
            else:
                trade_results["win_mean/std"] = (
                    df_returns_desc["win"]["mean"]
                    / df_returns_desc["win"]["std"]
                )
            trade_results["trades/week"] = trades_per_week
            trade_results["days_held_mean"] = df_returns_desc["loc_diff"][
                "mean"
            ]
            trade_results["days_held_std"] = df_returns_desc["loc_diff"]["std"]
            with np.errstate(divide="ignore"):
                trade_results["days_held_mean/std"] = (
                    df_returns_desc["loc_diff"]["mean"]
                    / df_returns_desc["loc_diff"]["std"]
                )

            file_tmp = path_data_dump + "trade_results.txt"
            with open(file_tmp, "w") as f:
                # use json.loads to do the reverse
                f.write(json.dumps(trade_results))
            pickle_dump(trade_results, path_data_dump, "trade_results")
            if verbose:
                print(f"trade_results: {trade_results}")
        if verbose:
            print("-" * 20 + " return function end " + "-" * 20, "\n")

    return trade_results


# endregion functions


# region +++ Main +++
# region +++ select inputs +++
# ########## Colab_change
path_symbols_data = 'C:/Users/ping/Desktop/test/'
# path_symbols_data = \
#     'C:/Users/ping/Google Drive/stocks/MktCap2b_AUMtop1200/'  # NOQA
# path_symbols_data = "/content/drive/MyDrive/stocks/MktCap2b_AUMtop1200/"
# ########## Colab_change

# +++++++++ takes 7.5 hours to run 0-5 days_trade_delay +++++++++
# delays in days between date-of-signal and date-of-trade
days_trade_delay_0 = 0
days_trade_delay_1 = 1
days_trade_delay_2 = 2
days_trade_delay_3 = 3
days_trade_delay_4 = 4
days_trade_delay_5 = 5
days_trade_delay_6 = 6
days_trade_delay_7 = 7
days_trade_delay_8 = 8
days_trade_delay_9 = 9
days_trade_delay_10 = 10
# set_days_trade_delay = [days_trade_delay_1, days_trade_delay_2]
set_days_trade_delay = [
    days_trade_delay_0,
    days_trade_delay_1,
    days_trade_delay_2,
    days_trade_delay_3,
    days_trade_delay_4,
    days_trade_delay_5,
    days_trade_delay_6,
    days_trade_delay_7,
    days_trade_delay_8,
    days_trade_delay_9,
    days_trade_delay_10,
]

pct_bid_ask_spread = 0.03  # applies to buy/sell price
drop_top_pct_CAGR = 0  # percent of the best CAGR_% rows to drop
drop_bottom_pct_CAGR = 0  # percent of the worst CAGR_% rows to drop

# subset limits on date_buy, increase 1 month for every calender month
date_buy_older_limit = None
date_buy_newer_limit = None
# date_buy_older_limit = '2020-06-01'
# date_buy_newer_limit = None

# verbose = True
verbose = False

file_df_results1 = "df_ps1n2_results"
file_df_results2 = "df_ps1n2_results_stock"
file_df_results3 = "df_ps1n2_results_etf"
# set_file_df_results = [file_df_results1]
set_file_df_results = [file_df_results1, file_df_results2, file_df_results3]

cols1 = "date_end, k12, k11, k10, k9, k8, k7, k6, k5, k4, k3, k2, k1"
cols2 = "date_end, k12, k11, k10, k9, k8, k7, k6, k5, k4, k3, k2"
cols3 = "date_end, k12, k11, k10, k9, k8, k7, k6, k5, k4, k3"
cols4 = "date_end, k12, k11, k10, k9, k8, k7, k6, k5, k4"
cols5 = "date_end, k12, k11, k10, k9, k8, k7, k6, k5"
cols6 = "date_end, k12, k11, k10, k9, k8, k7, k6"
cols7 = "date_end, k12, k11, k10, k9, k8, k7"
cols8 = "date_end, k12, k11, k10, k9, k8"
cols9 = "date_end, k12, k11, k10, k9"
cols10 = "date_end, k12, k11, k10"
cols11 = "date_end, k12, k11"
cols12 = "date_end, k12"

# +++ selects columns to be combined
# set_cols = [cols1, cols2]
set_cols = [
    cols1,
    cols2,
    cols3,
    cols4,
    cols5,
    cols6,
    cols7,
    cols8,
    cols9,
    cols10,
    cols11,
    cols12,
]

iloc_offsets1 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
iloc_offsets2 = [2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24]
iloc_offsets3 = [3, 6, 9, 12, 15, 18, 21, 24, 27, 30, 33, 36]
iloc_offsets4 = [4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48]
iloc_offsets5 = [5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60]
iloc_offsets6 = [6, 12, 18, 24, 30, 36, 42, 48, 54, 60, 66, 72]
iloc_offsets7 = [7, 14, 21, 28, 35, 42, 49, 56, 63, 70, 77, 84]
iloc_offsets8 = [8, 16, 24, 32, 40, 48, 56, 64, 72, 80, 88, 96]
iloc_offsets9 = [9, 18, 27, 36, 45, 54, 63, 72, 81, 90, 99, 108]
iloc_offsets10 = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120]
# set_iloc_offsets = [iloc_offsets9, iloc_offsets10]
set_iloc_offsets = [
    iloc_offsets1,
    iloc_offsets2,
    iloc_offsets3,
    iloc_offsets4,
    iloc_offsets5,
    iloc_offsets6,
    iloc_offsets7,
    iloc_offsets8,
    iloc_offsets9,
    iloc_offsets10,
]

slice_stop1 = 5
slice_stop2 = 10
slice_stop3 = 15
slice_stop4 = 20
# set_slice_stops = [slice_stop1, slice_stop2]
set_slice_stops = [slice_stop1, slice_stop2, slice_stop3, slice_stop4]
# endregion +++ select inputs +++

path_data_dump = path_symbols_data + "VSCode_dump/"
path_archive = path_symbols_data + "~archive/"
date_now = dt.date.today().strftime("%Y-%m-%d")  # today's date as string

# create directory if it is not there
os.makedirs(os.path.dirname(path_data_dump), exist_ok=True)
os.makedirs(os.path.dirname(path_archive), exist_ok=True)

# region pandas display options
pd.set_option("display.max_colwidth", 50)
pd.set_option("display.width", 550)
pd.set_option("display.max_columns", 7)
# endregion pandas display options

# region create list of symbols with csv data
# read symbol into a list
symbols_with_csv_data = loadtxt(
    path_data_dump + "symbols_with_csv_data.txt", dtype=str, unpack=False
)
set_symbols_with_csv_data = set(symbols_with_csv_data)  # convert list to set
# endregion create list of symbols with csv data

file_df_results_current = ""  # check need re-load file_df_results

# output file to check running process
# truncate the file to zero length before opening
open(path_data_dump + "_progress_ps5_symbols_in_out.txt", "w")

counter = 0  # progress counter for loop iterations
for (
    file_df_results,
    slice_stop,
    days_trade_delay,
    iloc_offsets,
    cols,
) in itertools.product(
    set_file_df_results,
    set_slice_stops,
    set_days_trade_delay,
    set_iloc_offsets,
    set_cols,
):
    # region create subset parameters, e.g. iloc_offsets and 'k12:k11'
    # change from 'date_end, k12, k11' to ['date_end', 'k12', 'k11']
    columns = cols.split(", ")
    name_columns = columns[1] + ":" + columns[-1]  # e.g. 'k12:k1'
    # endregion create parameters to subset df_ps1n2_results

    # region create df_symbols_in_out
    if file_df_results != file_df_results_current:
        df_org = pickle_load(path_data_dump, file_df_results)

        # region select date_end within day_buy limits
        df_org = df_org.sort_values(by=["date_end"])
        if date_buy_older_limit is None:  # must sort by date_buy? or date_end
            date_buy_older_limit = df_org.iloc[0]["date_end"]
        if date_buy_newer_limit is None:  # must sort by date_buy? or date_end
            date_buy_newer_limit = df_org.iloc[-1]["date_end"]
        df_org = df_org.loc[
            (df_org["date_end"] >= date_buy_older_limit)
            & (df_org["date_end"] <= date_buy_newer_limit)
        ]
        # endregion select date_end within day_buy limits

    # select rows that meet iloc_offsets and slice_stop
    my_col1 = "str_iloc_offsets"
    my_col1_value = str(iloc_offsets)
    my_col2 = "slice_stop"
    my_col2_value = slice_stop
    df = df_org.loc[
        (df_org[my_col1] == my_col1_value) & (df_org[my_col2] == my_col2_value)
    ]
    my_subset = (
        my_col1
        + " == "
        + my_col1_value
        + ", "
        + my_col2
        + " == "
        + str(my_col2_value)
    )

    if verbose:
        print(
            "+" * 20
            + " Main: iteration to create df_symbols_in_out "
            + "+" * 20
        )
        print(f"slice_stop: {slice_stop}")
        print(f"iloc_offsets: {iloc_offsets}")
        print(f"columns: {columns}")
        print(f"name_columns: {name_columns}")
        print(f"file_df_results: {file_df_results}\n")
        print("df_org.columns:\n{}\n".format(df_org.columns))
        print(
            "df_org.str_iloc_offsets.unique():\n{}\n".format(
                df_org.str_iloc_offsets.unique()
            )
        )
        print(f"my_subset: {my_subset}\n")
        print(f"df_org:\n{df_org}\n")
        print(f"df after my_subset:\n{df}\n")

    # release df from memory
    df_org = pd.DataFrame()

    # if not df.empty:
    if (not df.empty) and (len(df) > 1):
        # select columns in cols, sort date from newest to oldest
        df = df[columns].sort_values(by=["date_end"], ascending=False)
        # add column, e.g. 'k12:k1', a list-of-lists, e.g. from 'k12'...'k1'
        df[name_columns] = df.loc[:, columns[1:]].values.tolist()
        # flatten list-of-lists in column, e.g 'k12:k1'
        df[name_columns] = df[[name_columns]].apply(
            flatten_list_of_lists, axis=1
        )

        # region data clean: remove symbols in df[name_columns],
        #  e.g. df['k12:k5'], that do not have csv data
        symbols_no_csv_data = []
        for myList in df[name_columns]:
            set_myList = set(myList)
            set_myList_no_csv_data = set_myList - set_symbols_with_csv_data
            symbols_no_csv_data.append(list(set_myList_no_csv_data))
            myList_with_csv_data = list(set_myList - set_myList_no_csv_data)

        # flatten symbols in list-of-lists to a list
        symbols_no_csv_data = [
            mySymbol for myList in symbols_no_csv_data for mySymbol in myList
        ]
        list_dump(
            symbols_no_csv_data, path_data_dump, "symbols_no_csv_data.txt"
        )
        if verbose:
            print(f"symbols_no_csv_data:\n{symbols_no_csv_data}\n")
        # endregion data clean

        # create column of previous row from name_columns, e.g. 'k12:k1'
        df["previous_row"] = df[name_columns].shift(-1)
        index_last_row = df.index.values[-1]
        # replace NaN in last row of column 'previous_row' with value
        #  in name_columns
        df.at[index_last_row, "previous_row"] = df.at[
            index_last_row, name_columns
        ]

        # +++ raise errors with date_end sort and df[name_columns].shift
        if df["previous_row"].iloc[0] != df[name_columns].iloc[1]:
            msg1 = "df['previous_row'].iloc[0] +++ DOES NOT EQUAL TO +++ "
            msg2 = f"df[{name_columns}].iloc[1]\n\n"
            msg3 = f"df['previous_row'].iloc[0] =\n{df['previous_row'].iloc[0]}\n\n"  # NOQA
            msg4 = f"df[{name_columns}].iloc[1] =\n{df[name_columns].iloc[1]}\n\n"  # NOQA
            msg5 = "column 'date_end' may not be sorted with ascending=False or\n"  # NOQA
            msg6 = "df['previous_row'] = df[name_columns].shift(-1) "
            msg7 = "is not shifted correctly"
            err_msg = msg1 + msg2 + msg3 + msg4 + msg5 + msg6 + msg7
            raise ValueError(err_msg)

        df[["symbols_in", "symbols_out"]] = df[
            [name_columns, "previous_row"]
        ].apply(list_diff, axis=1, result_type="expand")
        df_symbols_in_out = df.loc[
            :, ["date_end", "symbols_in", "symbols_out"]
        ]
        df_symbols_in_out.to_csv(path_data_dump + "df_symbols_in_out.csv")
        if verbose:
            print(f"Wrote to {path_data_dump}df_symbols_in_out.csv")
        pickle_dump(df_symbols_in_out, path_data_dump, "df_symbols_in_out")

        # created df_symbols_in_out, release df from memory
        df = pd.DataFrame()
        df_symbols_in_out = pd.DataFrame()
        # endregion create df_symbols_in_out

        # region output parameters to subset df_ps1n2_results
        #  to a df_trade_results.txt
        trade_results = dict(
            file=file_df_results,
            columns=name_columns,
            str_iloc_offsets=str(iloc_offsets),
            slice_stop=slice_stop,
            days_trade_delay=days_trade_delay,
        )

        file_tmp = path_data_dump + "trade_results.txt"
        with open(file_tmp, "w") as f:
            # use json.loads to do the reverse
            f.write(json.dumps(trade_results))
        pickle_dump(trade_results, path_data_dump, "trade_results")
        # endregion output parameters to subset df_ps1n2_results

        # region trades function takes df_symbols_in_out, with columns: index,
        #  date_end, symbols_in, symbols_out, and generates df_trades with
        #  columns: index, symbol, date_in, date_out, loc_date_in,
        #  loc_date_out, date_buy, date_sell, and loc_diff. Date_in is the
        #  date the symbol enters symbols_in in df_symbols_in_out. Date_out is
        #  the date symbol enters symbols_out. loc_date_in and loc_date_out
        #  are index locations of date_in and date_out. Date_buy and date_sell
        #  are date_in and date_out delayed by days_trade_delay. Loc_diff is
        #  holding period of the trade, i.e. loc_date_out - loc_date_in.
        trades(days_trade_delay=days_trade_delay, verbose=verbose)
        # endregion trades function

        # region return function adds buy/sell prices to df_trades. Half of the
        #  bid-ask-spread is applied to each of the buy/sell price. It
        #  calculates CAGR_% and win/lose for each trades and append them to
        #  df_trades and saves df_trades to df_trades_n_returns.
        #  df_returns is a subset of df_trades where a top/bottom percentage of
        #  data, sorted by CAGR_%, are dropped. df_returns is subset of
        #  df_returns where the date_buy is within the
        #  date_buy_older_limit/date_buy_newer_limit.
        #  The subset parameters, and trade statistics: CAGR_%, win/lose,
        #  trade_per_week are returned as dictionary to be appended to a
        #  dataframe.
        #  trade_results is a dictionary of function parameters
        #  and trade statistics from datafame.describe(). It is to be appended
        #  to a dataframe where the column names are keys and parameters and
        #  statistics are values
        trade_results = returns(
            pct_bid_ask_spread=pct_bid_ask_spread,
            drop_top_pct_CAGR=drop_top_pct_CAGR,
            drop_bottom_pct_CAGR=drop_bottom_pct_CAGR,
            date_buy_older_limit=date_buy_older_limit,
            date_buy_newer_limit=date_buy_newer_limit,
        )
        # endregion return function

        # region append_df to append trade_results to df_trade_results
        if bool(trade_results):  # True if trade_results is not empty
            pd.set_option("display.max_colwidth", 50)
            pd.set_option("display.width", 550)
            pd.set_option("display.max_columns", 12)

            # columns_drop_duplicate = \
            #     ['file', 'columns', 'str_iloc_offsets', 'slice_stop',
            #      'days_trade_delay', 'pct_bid_ask_spread',
            #      'drop_top_pct_CAGR',
            #      'drop_bottom_pct_CAGR', 'date_buy_older_limit',
            #      'date_buy_newer_limit']
            #
            # removed 'date_buy_newer_limit' so it will (or will not)
            # overwrite results from previous days
            columns_drop_duplicate = [
                "file",
                "columns",
                "str_iloc_offsets",
                "slice_stop",
                "days_trade_delay",
                "pct_bid_ask_spread",
                "drop_top_pct_CAGR",
                "drop_bottom_pct_CAGR",
                "date_buy_older_limit",
            ]

            filename_df = "df_trade_results"
            filename_df_archive = filename_df + "_" + date_now
            # df_trade_results is df_trade_results with appended trade_results
            df_trade_results = append_df_2(
                trade_results,
                columns_drop_duplicate,
                path_data_dump,
                filename_df,
                path_archive,
                filename_df_archive,
                verbose=False,
            )
            if verbose:
                print(df_trade_results.iloc[:, 0:5], "\n")
                print(df_trade_results.iloc[:, 5:10], "\n")
                print(df_trade_results.iloc[:, 10:19], "\n")
                print(
                    df_trade_results.iloc[:, 19:22],
                    "\n" f"{df_trade_results.columns}\n",
                )
                print(f"len(df_trade_results): {len(df_trade_results)}\n")
    # endregion append_df to append trade_results to df_trade_results

    # output file to check running process
    with open(path_data_dump + "_progress_ps5_symbols_in_out.txt", "a") as f:
        date_time = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(
            f"counter: {counter},  date_time: {date_time}\n"
            f"file_df_results: {file_df_results }\n"
            f"slice_stop: {slice_stop}\n"
            f"days_trade_delay: {days_trade_delay}\n"
            f"iloc_offsets: {iloc_offsets}\n"
            f"cols: {cols}\n",
            file=f,
        )
    counter += 1
# endregion +++ Main +++
