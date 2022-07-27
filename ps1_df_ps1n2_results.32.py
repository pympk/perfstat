# region program description (takes 54 minutes to run iloc_iteration = 2)
# v32 get_symbol_date_6 was get_symbol_date_5
# v31 change path: "G:/My Drive/.." was "C:/Users/ping/Google Drive/..."
# v30 replace symbols_with_csv_data with symbols_with_numeric_data_n_valid_dates
#  replace get_symbol_data_4 with get_symbol_data_6 that added check for
#  duplicates in date index)
# v29 change from get_symbol_data_3 to get_symbol_data_4 to check
#  symbol index for NaN
# v28 get_symbol_data_3 was get_symbol_data_2 to stop overflowing RAM
#  replaced symbols with symbols_with_csv_data
#  added function update_symbols_data
#  rename function ps1n2 to cals_perf_stats and removed function arguments
#  rewrite section 'read symbols' OHLCV .csv files from directory'
#  remove dfs in ps1n2's argument
#  change progress output file to _progress_df_ps1n2_results.txt
# v27 replaced path_source with  path_symbols_file, add date_time to
#  progress output, put df_perf_stats_sorted_1... & ..csv in verbose
#  added force_update_df_symbols_close.
# v26 add progress output to _progress_ps1n2.25.txt, suppress print with
#  verbose = False
# v25 def calc_perf_stats was def ps1n2, append_df_2 was append_df,
#  use to_pickle and read_pickle to store and retrieve dataframes
# v24 add my_path and place symbols csv files in OHLCV directory
# v23 add itertools to iterate over set_iloc_offsets and set_top_symbols_ranges
# v22 update OHLCV data current if not current
# v21 add START, END print statements to  for i, end_date in
#  enumerate(end_dates[::-1], start=-len(end_dates))
# v20 moved append run_results to function append_df
# v19 append run_results to df_run_result before drop_duplicates rows
# v18 add set_iloc_offsets to iterate on iloc_offsets
# v17 df_run_result.loc[index_to_keep] was df_run_result.iloc[index_to_keep]
# v16 add splitting dataframe df_ps1n2_results into two dataframes:
#  df_ps1n2_results_stock and df_ps1n2_results_etf, and writes them out to
#  pickle files and csv files. Rename ps1n2_results.csv to df_ps1n2_results.csv
# v.15 renamed df_ps1n2_results column 'iloc_offsets' to 'str_iloc_offsets'
#  so df_ps1n2_results can be subsetted by str_iloc_offsets
# v.14 drop rows with NaN section obsoleted by removing zeros and NaN
#  from df_close
# v.13 replace symb_perf_stats with symb_perf_stats_vectorized,
#  replace get_symbol_data with get_symbol_data_2
# v.12 execute symb_perf_stats(close) iff close does not have zeros or NaN
# v.11 Stop dropping symbols that have all_zeros_row_count from
#  df_symbols_row_count. Only calculate performance stats where symbol's close
#  does not have zero.
# v.10 add path_XOM for debug
# v.9 remove dfs_plot, use dfs_symbols_OHLCV_download for plotting.
# v.8 remove pickle_dump(sorted_symbol_n_count_tuples, path_data_dump,
#  'sorted_symbol_n_count_tuples_' + date_end)
# v.7 remove symbols with rows of zeros from dfs and list of symbols
# v.6 add ability to iterate thru multiple date_end_limit,
#  previous version can only run one date_end_limit
# v.5 add columns_drop_duplicate
# v.4 dropped duplicate rows in df_run_result
# v.3 added output for df_run_result, dfs_plot, dfs_symbols_OHLCV_download
# v.2 use largest offset(max_index_offset) to set date_start, date_end
#
# The program reads downloaded symbol data from YLoader.  It calculates the
# symbol’s performance (i.e. 'CAGR', 'UI', 'Std/UI', 'CAGR/Std', 'CAGR/UI')
# for each time period in "iloc_offsets".
#
# For each time period, it sorts the performance results by the criteria in
# “sort”, and appends the top sorted symbols, as specified in
# “top_symbols_range”, to “top_symbols_iloc_offsets_runs”.
# “top_symbols_iloc_offsets_runs” is a list that stores the top performing
# symbols for each iloc_offset time period. A symbol will likely to be in the
# list multiple times. For example, if Tesla is in the top symbol list for the
# last 18 days, it will likely to also be in the list for the last 36 days.
#
# Symbols in “top_symbols_iloc_offsets_runs” is sorted for unique symbols and
# the frequency of the symbol's occurrence in the list.  Symbols with the
# highest occurrence are the best performing symbols over the iloc_offset
# periods.
# endregion

# region import modules
import os
import ast
import pandas as pd
import numpy as np
import datetime as dt
import itertools

# ########## Colab_change
import sys
sys.path.append("G:/My Drive/py_files/python/perfstat/")
# sys.path.append("/content/drive/MyDrive/py_files/python/perfstat/")
# ########## Colab_change


# sys.path.append("C:/Users/ping/MyDrive/py_files/python/utils.py")
# sys.path.append("C:/Users/ping/MyDrive/py_files/python/utils.py")

from utils import list_dump, append_df_2
from utils import print_list_in_groups, pd_lists_intersection
from utils import pickle_dump, pickle_load, dates_within_limits, list_sort
from utils import symb_perf_stats_vectorized
from get_symbol_data_6 import get_symbol_data_6

# ########## Colab_change
my_path = "G:/My Drive/stocks/MktCap2b_AUMtop1200/"
# my_path = "/content/drive/MyDrive/stocks/MktCap2b_AUMtop1200/"
# ########## Colab_change
# endregion import modules


def calc_perf_stats():
    # v3 dropped dfs in argument
    # v2 use append_df_2 was append_df
    #
    date_start, date_end, iloc_date_start, iloc_date_end = dates_within_limits(
        date_index_all_dates,
        date_start_limit,
        end_date,
        iloc_offset=max_index_offset,
        verbose=verbose,
    )
    date_index = date_index_all_dates[iloc_date_start : (iloc_date_end + 1)]

    if verbose:
        print("&" * 40)
        print(f"date_index: {date_index}")
        print(
            f"end_date: {end_date}, iloc_date_start: {iloc_date_start}, "
            f"iloc_date_end: {iloc_date_end}"
        )
        print("&" * 40, "\n" * 2)

    # region calculate performance stats for each iloc_offset
    # list to store top sorted symbols from each iloc_offset run,
    #   not a list-of-lists
    top_symbols_iloc_offsets_runs = []
    # calculate performance stats for symbols for each iloc_offset duration
    for iloc_offset in iloc_offsets:
        if verbose:
            print("\n{}".format("=" * 78))
            print(
                "+ Calculate Perf. Stats: iloc_offset = {} days, Sorted by {}\n".format(  # NOQA
                    iloc_offset, sort
                )
            )

        # csv output file name for performance stats
        filename_csv = "df_perf_stats_sorted_" + str(iloc_offset) + ".csv"
        # list of tuple which stores the performance data of a symbol
        caches_perf_stats = []
        # calculate start and end dates, and their respective index locations
        (
            date_start,
            date_end,
            iloc_date_start,
            iloc_date_end,
        ) = dates_within_limits(
            date_index,
            date_start_limit,
            end_date,
            iloc_offset,
            verbose=verbose,
        )
        # convert dates from timestamp to string for
        #  def get_symbol_data_from_dir
        date_start = date_start.strftime("%Y-%m-%d")
        date_end = date_end.strftime("%Y-%m-%d")
        if verbose:
            print(
                print_format3.format("date_start_limit", str(date_start_limit))
            )
            print(print_format3.format("end_date", str(end_date)))
            print(print_format3.format("iloc_offset", str(iloc_offset)))
            print(
                "date_start: {:<10s}, type: {}".format(
                    date_start, type(date_start)
                )
            )
            print(
                "date_end:   {:<10s}, type: {}".format(
                    str(date_end), type(date_end)
                )
            )
            print(
                "date_today: {:<10s}, type: {}\n".format(
                    str(dt.date.today()), type(dt.date.today())
                )
            )

        df_close = df_symbols_close[date_start:date_end]
        date_first = df_close.index[0].strftime("%Y-%m-%d")
        date_last = df_close.index[-1].strftime("%Y-%m-%d")

        # symbols with closing price zero
        symbol_close_with_zeros = df_close.columns[
            (df_close == 0).any()
        ].tolist()
        # symbols with closing price NaN
        symbol_close_with_NaN = df_close.columns[
            np.isnan(df_close).any()
        ].tolist()
        list_dump(
            symbol_close_with_zeros,
            path_data_dump,
            "symbol_close_with_zeros.txt",
        )
        list_dump(
            symbol_close_with_NaN, path_data_dump, "symbol_close_with_NaN.txt"
        )

        df_volume = df_symbols_volume[date_start:date_end]
        symbol_volume_with_zeros = df_volume.columns[
            (df_volume == 0).any()
        ].tolist()
        symbol_volume_with_NaN = df_volume.columns[
            np.isnan(df_volume).any()
        ].tolist()
        list_dump(
            symbol_volume_with_zeros,
            path_data_dump,
            "symbol_volume_with_zeros.txt",
        )
        list_dump(
            symbol_volume_with_NaN,
            path_data_dump,
            "symbol_volume_with_NaN.txt",
        )
        if verbose:
            print(
                "symbols close with zeros: {}".format(symbol_close_with_zeros)
            )
            print("symbols close with Nan: {}".format(symbol_close_with_NaN))
            print(
                "symbols volume with zeros: {}".format(
                    symbol_volume_with_zeros
                )
            )
            print("symbols volume with Nan: {}".format(symbol_volume_with_NaN))

        # data cleaning: list of columns/symbols to drop from df_close
        columns_drop = (
            symbol_close_with_zeros
            + symbol_close_with_NaN
            + symbol_volume_with_zeros
            + symbol_volume_with_NaN
        )
        columns_drop = list(set(columns_drop))  # get unique value in list
        # drop columns with zeros and NaNs
        df_close = df_close.drop(columns_drop, axis=1)
        list_dump(
            columns_drop,
            path_data_dump,
            "symbols_with_zeros_or_NaNs_dropped_from_perf_stat.txt",
        )

        # calculate performance stats
        (
            symbols_perf_stats,
            period_yr,
            drawdown,
            UI,
            max_drawdown,
            returns_std,
            Std_UI,
            CAGR,
            CAGR_Std,
            CAGR_UI,
        ) = symb_perf_stats_vectorized(df_close)

        caches_perf_stats = []
        for symbol in symbols_perf_stats:
            cache = (
                symbol,
                date_first,
                date_last,
                period_yr,
                CAGR[symbol],
                UI[symbol],
                Std_UI[symbol],
                CAGR_Std[symbol],
                CAGR_UI[symbol],
            )
            # append performance results (tuple) to caches_perf_stats (list)
            caches_perf_stats.append(cache)

        # create dataframe column names for performance stats
        column_names = [
            "symbol",
            "first date",
            "last date",
            "Year",
            "CAGR",
            "UI",
            "Std/UI",
            "CAGR/Std",
            "CAGR/UI",
        ]
        # write symbols' performance stats to dataframe
        df_perf_stats = pd.DataFrame(caches_perf_stats, columns=column_names)
        if verbose:
            print("finished calculating performance stats \n")

        # sort dataframe
        df_perf_stats_sorted = df_perf_stats.sort_values(
            by=[sort], ascending=False
        )

        if verbose:
            print("df_perf_stats_sorted is sorted by {}\n".format(sort))

            # write df_perf_stats_sorted to file
            df_perf_stats_sorted.to_pickle(
                path_data_dump + "df_perf_stats_sorted_" + str(iloc_offset)
            )
            # write file e.g. 'df_perf_stats_sorted_36.csv'
            df_perf_stats_sorted.to_csv(path_data_dump + filename_csv)
            # if verbose:
            print("Wrote {} to {} \n".format(filename_csv, path_data_dump))

            # print top performing symbols for the iloc_offset duration
            print(
                'Top {}-{} symbols of df_perf_stats_sorted, sorted by "{}": \n'.format(  # NOQA
                    top_symbols_range.start, top_symbols_range.stop, sort
                )
            )  # NOQA
            print("{}\n".format(df_perf_stats_sorted[top_symbols_range]))
        # get a slice(top_symbols_range) of top symbols from sort
        symbols_sorted = list(
            df_perf_stats_sorted.symbol[top_symbols_range].values
        )

        if verbose:
            # print top performing symbols for the iloc_offset duration
            #  in groups
            print(
                'symbols in df_perf_stats_sorted_{} sorted by "{}", in groups of {}:'.format(  # NOQA
                    iloc_offset, sort, print_group_size
                )
            )
            print_list_in_groups(symbols_sorted, print_group_size)
            print("symbols_sorted:\n{}".format(symbols_sorted))
            print("iloc_offset: {}\n".format(iloc_offset))
            print(
                f"- Calculate Perf. Stats: "
                f"iloc_offset = {iloc_offset} days, Sorted by {sort}"
            )

            print("{}\n".format("-" * 78))

            # write top performing symbols for the iloc_offset duration to
            #  files
            file_txt = "top_symbols_sorted_" + str(iloc_offset) + ".txt"
            list_dump(symbols_sorted, path_data_dump, file_txt)
            filename_pickle = "top_symbols_sorted_" + str(iloc_offset)
            pickle_dump(symbols_sorted, path_data_dump, filename_pickle)

        for symbol in symbols_sorted:
            top_symbols_iloc_offsets_runs.append(symbol)
        if verbose:
            print(
                f"appended symbols in {filename_pickle} "
                f"to top_symbols_iloc_offsets_runs"
            )
    # endregion calculate performance stats for each iloc_offset

    # region sort top_symbols_iloc_offsets_runs into unique symbol and corresponding counts  # NOQA
    (
        sorted_unique_symbols,
        sorted_unique_counts,
        sorted_symbol_n_count_tuples,
    ) = list_sort(top_symbols_iloc_offsets_runs, verbose=False)
    if verbose:
        print(
            "sorted top_symbols_iloc_offsets_runs into "
            "unique symbols and counts, "
        )
        print(" and wrote each unique symbol and count pair as tuple to ")
        print(" sorted_symbol_n_count_tuples\n")

    count_total_unique_symbols = len(sorted_unique_symbols)
    count_total_symbols_all_lists = len(top_symbols_iloc_offsets_runs)
    # total number of symbols
    count_total_symbols = len(symbols_with_numeric_data_n_valid_dates)

    pct_unique_to_total = (
        100 * count_total_unique_symbols / count_total_symbols
    )
    pct_all_lists_to_total = (
        100 * count_total_symbols_all_lists / count_total_symbols
    )
    if verbose:
        print("symbols counts:")
        print("unique symbols: {}".format(count_total_unique_symbols))
        print("lists symbols: {}".format(count_total_symbols_all_lists))
        print("all symbols: {}\n".format(count_total_symbols))
        print("percentage of symbols counts:")
        print("unique symbols / all symbols: {} %".format(pct_unique_to_total))
        print(f"lists symbols / all symbols: {pct_all_lists_to_total} %\n")

    if verbose:
        print(
            "top_symbols_iloc_offsets_runs:\n{}\n".format(
                top_symbols_iloc_offsets_runs
            )
        )
        print("sorted_unique_symbols:\n{}\n".format(sorted_unique_symbols))
        print("sorted_unique_counts:\n{}\n".format(sorted_unique_counts))
        print(
            "sorted_symbol_n_count_tuples:\n{}\n".format(
                sorted_symbol_n_count_tuples
            )
        )
    # endregion

    # region initialize keys & counts
    list_keys_all = []
    list_counts_all = []
    list_keys_12 = []
    list_counts_12 = []
    list_keys_11 = []
    list_counts_11 = []
    list_keys_10 = []
    list_counts_10 = []
    list_keys_9 = []
    list_counts_9 = []
    list_keys_8 = []
    list_counts_8 = []
    list_keys_7 = []
    list_counts_7 = []
    list_keys_6 = []
    list_counts_6 = []
    list_keys_5 = []
    list_counts_5 = []
    list_keys_4 = []
    list_counts_4 = []
    list_keys_3 = []
    list_counts_3 = []
    list_keys_2 = []
    list_counts_2 = []
    list_keys_1 = []
    list_counts_1 = []
    # endregion

    # region unpack sorted_symbol_n_count_tuples into symbol and occurrence
    if verbose:
        print("Unpack sorted_symbol_n_count_tuples")
        print("+" * 78)
    for i in sorted_symbol_n_count_tuples:
        if verbose:
            print(print_format3.format(str(i[0]), str(i[1])))
        list_keys_all.append(i[0])
        list_counts_all.append(i[1])
        if i[1] == 12:
            list_keys_12.append(i[0])
            list_counts_12.append(i[1])
        if i[1] == 11:
            list_keys_11.append(i[0])
            list_counts_11.append(i[1])
        if i[1] == 10:
            list_keys_10.append(i[0])
            list_counts_10.append(i[1])
        if i[1] == 9:
            list_keys_9.append(i[0])
            list_counts_9.append(i[1])
        if i[1] == 8:
            list_keys_8.append(i[0])
            list_counts_8.append(i[1])
        if i[1] == 7:
            list_keys_7.append(i[0])
            list_counts_7.append(i[1])
        if i[1] == 6:
            list_keys_6.append(i[0])
            list_counts_6.append(i[1])
        if i[1] == 5:
            list_keys_5.append(i[0])
            list_counts_5.append(i[1])
        if i[1] == 4:
            list_keys_4.append(i[0])
            list_counts_4.append(i[1])
        if i[1] == 3:
            list_keys_3.append(i[0])
            list_counts_3.append(i[1])
        if i[1] == 2:
            list_keys_2.append(i[0])
            list_counts_2.append(i[1])
        if i[1] == 1:
            list_keys_1.append(i[0])
            list_counts_1.append(i[1])
    if verbose:
        print("+" * 78)
    # endregion unpack sorted_symbol_n_count_tuples into symbol occurrence freq

    # region sum symbol's occurrence frequency into 4 buckets,
    #  calculate occurrence frequency of each bucket as a percentage
    symbol_count_keys_12_11_10 = (
        len(list_keys_12) + len(list_keys_11) + len(list_keys_10)
    )
    symbol_count_keys_9_8_7 = (
        len(list_keys_9) + len(list_keys_8) + len(list_keys_7)
    )
    symbol_count_keys_6_5_4 = (
        len(list_keys_6) + len(list_keys_5) + len(list_keys_4)
    )
    symbol_count_keys_3_2_1 = (
        len(list_keys_3) + len(list_keys_2) + len(list_keys_1)
    )
    symbol_count_keys_all = (
        symbol_count_keys_12_11_10
        + symbol_count_keys_9_8_7
        + symbol_count_keys_6_5_4
        + symbol_count_keys_3_2_1
    )
    # calculate percentage of occurrence freqency of each bucket
    pct_symbol_count_keys_12_11_10 = (
        100 * symbol_count_keys_12_11_10 / symbol_count_keys_all
    )
    pct_symbol_count_keys_9_8_7 = (
        100 * symbol_count_keys_9_8_7 / symbol_count_keys_all
    )
    pct_symbol_count_keys_6_5_4 = (
        100 * symbol_count_keys_6_5_4 / symbol_count_keys_all
    )
    pct_symbol_count_keys_3_2_1 = (
        100 * symbol_count_keys_3_2_1 / symbol_count_keys_all
    )
    pct_all_counts_as_dfs_symbols = (
        symbol_count_keys_all / count_total_symbols * 100
    )
    if verbose:
        print("\n")
        print("Top {} symbols and counts in iloc_offsets".format(sort))
        print(
            print_format4.format(
                "count brackets", "no. of symbols", "no. of symbols"
            )
        )

        print("+" * 78)
        print(
            print_format4.format(
                "counts: 12, 11, 10",
                str(symbol_count_keys_12_11_10),
                str(pct_symbol_count_keys_12_11_10),
            )
        )
        print(
            print_format4.format(
                "counts: 9, 8, 7",
                str(symbol_count_keys_9_8_7),
                str(pct_symbol_count_keys_9_8_7),
            )
        )
        print(
            print_format4.format(
                "counts: 6, 5, 4",
                str(symbol_count_keys_6_5_4),
                str(pct_symbol_count_keys_6_5_4),
            )
        )
        print(
            print_format4.format(
                "counts: 3, 2, 1",
                str(symbol_count_keys_3_2_1),
                str(pct_symbol_count_keys_3_2_1),
            )
        )
        print("+" * 78)
        print(print_format5.format("total_counts", str(symbol_count_keys_all)))
        print(
            print_format5.format("total_dfs_symbols", str(count_total_symbols))
        )
        print(
            print_format5.format(
                "total_counts / total_dfs_symbols, %",
                str(pct_all_counts_as_dfs_symbols),
            )
        )
        print("\n")
    # endregion sum symbol's occurrence frequency into 4 buckets,

    # region print symbols for ps4_plot_symbols to plot
    if verbose:
        print(
            "{} sorted unique symbol, count = all: \n{}".format(
                top_symbols_iloc_offsets_runs, list_keys_all
            )
        )
        print(
            "{} sorted unique symbol count: \n{}\n".format(
                top_symbols_iloc_offsets_runs, list_counts_all
            )
        )

    # region print symbols in each count
    if verbose:
        print("Symbols in Each Count")
        print("+" * 78)
        print(
            "{} sorted unique symbol, count = 12: \n{}".format(
                "top_symbols_iloc_offsets_runs", list_keys_12
            )
        )
        print(
            "{} sorted unique symbol, count = 12: \n{}\n".format(
                "top_symbols_iloc_offsets_runs", list_counts_12
            )
        )
        print(
            "{} sorted unique symbol, count = 11: \n{}".format(
                "top_symbols_iloc_offsets_runs", list_keys_11
            )
        )
        print(
            "{} sorted unique symbol, count = 11: \n{}\n".format(
                "top_symbols_iloc_offsets_runs", list_counts_11
            )
        )
        print(
            "{} sorted unique symbol, count = 10: \n{}".format(
                "top_symbols_iloc_offsets_runs", list_keys_10
            )
        )
        print(
            "{} sorted unique symbol, count = 10: \n{}\n".format(
                "top_symbols_iloc_offsets_runs", list_counts_10
            )
        )
        print(
            "{} sorted unique symbol, count = 9: \n{}".format(
                "top_symbols_iloc_offsets_runs", list_keys_9
            )
        )
        print(
            "{} sorted unique symbol, count = 9: \n{}\n".format(
                "top_symbols_iloc_offsets_runs", list_counts_9
            )
        )
        print(
            "{} sorted unique symbol, count = 8: \n{}".format(
                "top_symbols_iloc_offsets_runs", list_keys_8
            )
        )
        print(
            "{} sorted unique symbol, count = 8: \n{}\n".format(
                "top_symbols_iloc_offsets_runs", list_counts_8
            )
        )
        print(
            "{} sorted unique symbol, count = 7: \n{}".format(
                "top_symbols_iloc_offsets_runs", list_keys_7
            )
        )
        print(
            "{} sorted unique symbol, count = 7: \n{}\n".format(
                "top_symbols_iloc_offsets_runs", list_counts_7
            )
        )
        print(
            "{} sorted unique symbol, count = 6: \n{}".format(
                "top_symbols_iloc_offsets_runs", list_keys_6
            )
        )
        print(
            "{} sorted unique symbol, count = 6: \n{}\n".format(
                "top_symbols_iloc_offsets_runs", list_counts_6
            )
        )
        print(
            "{} sorted unique symbol, count = 5: \n{}".format(
                "top_symbols_iloc_offsets_runs", list_keys_5
            )
        )
        print(
            "{} sorted unique symbol, count = 5: \n{}\n".format(
                "top_symbols_iloc_offsets_runs", list_counts_5
            )
        )
        print(
            "{} sorted unique symbol, count = 4: \n{}".format(
                "top_symbols_iloc_offsets_runs", list_keys_4
            )
        )
        print(
            "{} sorted unique symbol, count = 4: \n{}\n".format(
                "top_symbols_iloc_offsets_runs", list_counts_4
            )
        )
        print(
            "{} sorted unique symbol, count = 3: \n{}".format(
                "top_symbols_iloc_offsets_runs", list_keys_3
            )
        )
        print(
            "{} sorted unique symbol, count = 3: \n{}\n".format(
                "top_symbols_iloc_offsets_runs", list_counts_3
            )
        )
        print(
            "{} sorted unique symbol, count = 2: \n{}".format(
                "top_symbols_iloc_offsets_runs", list_keys_2
            )
        )
        print(
            "{} sorted unique symbol, count = 2: \n{}\n".format(
                "top_symbols_iloc_offsets_runs", list_counts_2
            )
        )
        print(
            "{} sorted unique symbol, count = 1: \n{}".format(
                "top_symbols_iloc_offsets_runs", list_keys_1
            )
        )
        print(
            "{} sorted unique symbol, count = 1: \n{}".format(
                "top_symbols_iloc_offsets_runs", list_counts_1
            )
        )
        print("+" * 78)
    # endregion print symbols in each count
    # endregion unpack sorted_symbol_n_count_tuples into symbol and occurrence

    # region print sorted_symbol_n_count_tuple to
    if verbose:
        print(
            "sorted_symbol_n_count_tuples:\n{}\n".format(
                sorted_symbol_n_count_tuples
            )
        )
        print(
            "- Accumulate and Flatten Lists of top symbols "
            "for each iloc_offset"
        )
        print("{}\n".format("=" * 78))
    # endregion

    # region write results to ps1n2_results.txt
    with open(path_data_dump + filename_ps1n2_results, "w") as f:
        f.write(print_format6.format("date_end", str(date_end)))
        f.write(print_format6.format("list_keys_12", str(list_keys_12)))
        f.write(print_format6.format("list_keys_11", str(list_keys_11)))
        f.write(print_format6.format("list_keys_10", str(list_keys_10)))
        f.write(print_format6.format("list_keys_9", str(list_keys_9)))
        f.write(print_format6.format("list_keys_8", str(list_keys_8)))
        f.write(print_format6.format("list_keys_7", str(list_keys_7)))
        f.write(print_format6.format("list_keys_6", str(list_keys_6)))
        f.write(print_format6.format("list_keys_5", str(list_keys_5)))
        f.write(print_format6.format("list_keys_4", str(list_keys_4)))
        f.write(print_format6.format("list_keys_3", str(list_keys_3)))
        f.write(print_format6.format("list_keys_2", str(list_keys_2)))
        f.write(print_format6.format("list_keys_1", str(list_keys_1)))
        f.write(
            print_format6.format("symb_uniq", str(count_total_unique_symbols))
        )
        f.write(
            print_format6.format(
                "symb_lists", str(count_total_symbols_all_lists)
            )
        )  # NOQA
        f.write(print_format6.format("symb_all", str(count_total_symbols)))
        f.write(print_format6.format("pct_uniq_all", str(pct_unique_to_total)))
        f.write(
            print_format6.format("pct_lists_all", str(pct_all_lists_to_total))
        )
        f.write(print_format6.format("iloc_offsets", str(iloc_offsets)))
        f.write(
            print_format6.format("top_symbols_range", str(top_symbols_range))
        )
        f.write(print_format6.format("path", str(my_path)))
        if verbose:
            print("Top {} symbols and counts in iloc_offsets".format(sort))
            print("+" * 78)
            print(print_format7.format("list_keys_12", str(list_keys_12)))
            print(print_format7.format("list_keys_11", str(list_keys_11)))
            print(print_format7.format("list_keys_10", str(list_keys_10)))
            print(print_format7.format("list_keys_9", str(list_keys_9)))
            print(print_format7.format("list_keys_8", str(list_keys_8)))
            print(print_format7.format("list_keys_7", str(list_keys_7)))
            print(print_format7.format("list_keys_6", str(list_keys_6)))
            print(print_format7.format("list_keys_5", str(list_keys_5)))
            print(print_format7.format("list_keys_4", str(list_keys_4)))
            print(print_format7.format("list_keys_3", str(list_keys_3)))
            print(print_format7.format("list_keys_2", str(list_keys_2)))
            print(print_format7.format("list_keys_1", str(list_keys_1)))
            print(
                print_format7.format(
                    "symb_uniq", str(count_total_unique_symbols)
                )
            )
            print(
                print_format7.format(
                    "symb_lists", str(count_total_symbols_all_lists)
                )
            )  # NOQA
            print(print_format7.format("symb_all", str(count_total_symbols)))
            print(
                print_format7.format("pct_uniq_all", str(pct_unique_to_total))
            )
            print(
                print_format7.format(
                    "pct_lists_all", str(pct_all_lists_to_total)
                )
            )
            print(print_format7.format("iloc_offsets", str(iloc_offsets)))
            print(
                print_format7.format(
                    "top_symbols_range", str(top_symbols_range)
                )
            )
            print(print_format7.format("path", str(my_path)))

    if verbose:
        print(
            f"\nTop {sort} symbols and counts in iloc_offsets "
            f"are saved in file:"
        )
        print("{}{}\n".format(path_data_dump, filename_ps1n2_results))
    # endregion

    # region run_results format
    run_results = "'{}':'{}', \
    '{}':{}, \
    '{}':{}, \
    '{}':{}, \
    '{}':{}, \
    '{}':{}, \
    '{}':{}, \
    '{}':{}, \
    '{}':{}, \
    '{}':{}, \
    '{}':{}, \
    '{}':{}, \
    '{}':{}, \
    '{}':{}, \
    '{}':{}, \
    '{}':{}, \
    '{}':{}, \
    '{}':{}, \
    '{}':{}, \
    '{}':{}, \
    '{}':{}, \
    '{}':'{}'".format(
        "date_end",
        date_end,
        "k12",
        list_keys_12,
        "k11",
        list_keys_11,
        "k10",
        list_keys_10,
        "k9",
        list_keys_9,
        "k8",
        list_keys_8,
        "k7",
        list_keys_7,
        "k6",
        list_keys_6,
        "k5",
        list_keys_5,
        "k4",
        list_keys_4,
        "k3",
        list_keys_3,
        "k2",
        list_keys_2,
        "k1",
        list_keys_1,
        "symb_uniq",
        count_total_unique_symbols,
        "symb_lists",
        count_total_symbols_all_lists,  # NOQA
        "symb_all",
        count_total_symbols,
        "pct_uniq_all",
        pct_unique_to_total,
        "pct_lists_all",
        pct_all_lists_to_total,
        "str_iloc_offsets",
        str_iloc_offsets,
        "slice_start",
        top_symbols_range.start,
        "slice_stop",
        top_symbols_range.stop,
        "path",
        my_path,
    )

    print(
        "top_symbols_range({}): {}".format(
            type(str(top_symbols_range)), str(top_symbols_range)
        )
    )
    run_results = "{" + run_results + "}"  # need this line to add bracket
    # print('run_results({}):\n{}'.format(type(run_results), run_results))

    # convert run_results from 'str' to 'dict'
    run_results = ast.literal_eval(run_results)
    print("run_results({}):\n {}".format(type(run_results), run_results))
    print("\n")
    # endregion

    # region write and print df_run_result to csv file
    df_run_result = append_df_2(
        run_results,
        columns_drop_duplicate,
        path_data_dump,
        filename_df,
        path_archive,
        filename_df_archive,
        verbose,
    )

    if verbose:
        print(
            "df_run_result(shape = {}):\n{}\n".format(
                df_run_result.shape, df_run_result
            )
        )
        print(df_run_result.iloc[:, np.r_[0:10]], "\n")
        print(df_run_result.iloc[:, np.r_[10:20]], "\n")
        print(df_run_result.iloc[:, np.r_[20:22]], "\n")
        print("+" * 78)

    # print time when loop finished
    if verbose:
        print(
            "---- end_date {} loop finished on ----: {}".format(
                end_date, dt.datetime.now()
            )
        )
    # endregion write and print df_run_result to csv file


def update_symbols_data():
    (
        dfs,
        symbols_with_numeric_data_n_valid_dates,
        symbols_no_csv_data,
        symbols_duplicate_dates,
        df_symbols_close,
        df_symbols_volume,
    ) = get_symbol_data_6(
        dir_path=path_symbols_data,
        file_symbols=file_symbols,
        date_start=None,
        date_end=None,
        index_symbol=index_symbol,
        verbose=verbose,
    )
    pickle_dump(dfs, path_data_dump, "dfs_symbols_OHLCV_download")
    pickle_dump(
        symbols_with_numeric_data_n_valid_dates,
        path_data_dump,
        "symbols_with_numeric_data_n_valid_dates",
    )
    # pickle_dump(symbols, path_data_dump, "symbols")
    pickle_dump(symbols_no_csv_data, path_data_dump, "symbols_no_csv_data")
    pickle_dump(
        symbols_duplicate_dates, path_data_dump, "symbols_duplicate_dates"
    )
    list_dump(
        symbols_with_numeric_data_n_valid_dates,
        path_data_dump,
        "symbols_with_numeric_data_n_valid_dates.txt",
    )
    list_dump(symbols_no_csv_data, path_data_dump, "symbols_no_csv_data.txt")
    list_dump(
        symbols_duplicate_dates, path_data_dump, "symbols_duplicate_dates.txt"
    )
    df_symbols_close.to_pickle(path_data_dump + "df_symbols_close")
    df_symbols_volume.to_pickle(path_data_dump + "df_symbols_volume")
    df_symbols_close.to_csv(path_data_dump + "df_symbols_close.csv")
    df_symbols_volume.to_csv(path_data_dump + "df_symbols_volume.csv")
    dfs.clear()  # release dfs from memory

    return (
        symbols_with_numeric_data_n_valid_dates,
        symbols_no_csv_data,
        symbols_duplicate_dates,
        df_symbols_close,
        df_symbols_volume,
    )


# region main
# region +++ set before each run +++
path_symbols_data = my_path + "OHLCV/"  # NOQA
path_symbols_file = my_path + "source/"

# path to symbols text file
# file_symbols = path_symbols_file + "test.txt"
file_symbols = path_symbols_file + "2021_Top1200_MktCap_n_AUM.txt"
file_stocks_Top1200_MktCap = path_symbols_file + "2021_MktCapTop1200.csv"
file_etfs_Top1200_AUM = path_symbols_file + "2021_AUMtop1200.csv"

# date_end_limit is the last date used in performance stats calculation
# date_end_limit = "2020-10-23"  # test
# # date_end_limit = '2013-02-06'  # next colab run
date_end_limit = "2022-04-26"
# date_end_limit = dt.date.today().strftime("%Y-%m-%d")

# re-load symbols' OHLCV data
# force_update_df_symbols_close = True
force_update_df_symbols_close = False

# ++++ takes 9 hours to run 100 iloc_iteration ++++
# iloc_iteration is number of dates to go back from date_end_limit
#  took 19 hours for 50 iterations
# iloc_iteration = 22  # iloc_iteration = 0 results in 1 iteration
iloc_iteration = 1  # iloc_iteration = 0 results in 1 iteration, 1.3 hrs for 5
# iloc_iteration = 1000  # iloc_iteration = 0 results in 1 iteration

# ++++ set_iloc_offsets: set of iloc_offsets to iterate ++++
# iloc_offset=2 has 2 intervals(i.e. 3 rows of data)
# region ++++ code to generate iloc_offests ++++
# myNum = 10
# myList = [*range(myNum, 12*myNum+1, myNum)]
# print(f'iloc_offsets{myNum} = {myList}')
# print(f'len(list): {len(myList)}')
# endregion ++++ code to generate iloc_offests ++++
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
set_iloc_offsets = [
    # iloc_offsets1,
    # iloc_offsets2,
    # iloc_offsets3,
    # iloc_offsets4,
    # iloc_offsets5,
    # iloc_offsets6,
    # iloc_offsets7,
    # iloc_offsets8,
    # iloc_offsets9,
    iloc_offsets10,
]

# slice of the top performaning symbols to keep for each iloc_offset
top_symbols_range1 = slice(0, 5)
top_symbols_range2 = slice(0, 10)
top_symbols_range3 = slice(0, 15)
top_symbols_range4 = slice(0, 20)
# set_top_symbols_ranges = [top_symbols_range2]
set_top_symbols_ranges = [
    top_symbols_range1,
    # top_symbols_range2,
    # top_symbols_range3,
    # top_symbols_range4,
]
# endregion +++ set before each run +++

# region constants
# data dump directory paths
path_data_dump = my_path + "VSCode_dump/"
path_archive = my_path + "~archive/"
# create directory if it is not there
os.makedirs(os.path.dirname(path_data_dump), exist_ok=True)
os.makedirs(os.path.dirname(path_archive), exist_ok=True)
# range of top performing symbols to select for each set of start and end dates

# date_start_limit is the first date used in performance stats calculation
#  if None, iloc_offset and date_end_limit set date_start_limit
date_start_limit = None
days_plot = 252  # number of days to plot, use by script ps4...
# use largest offset to set date_start, date_end
max_index_offset = max(days_plot, np.max(set_iloc_offsets))

date_now = dt.date.today().strftime("%Y-%m-%d")  # today's date as string
max_NaN_per_row = 0  # parameter for function remove_NaN_rows
print_group_size = 10  # print sorted symbols in groups
index_symbol = "XOM"  # use Exxon's date index to re-index other symbols

verbose = False  # True prints more outputs
# verbose = True  # True prints more outputs

# sort criteria for performance: 'CAGR', 'Std/UI', 'CAGR/Std', 'CAGR/UI'
sort = "CAGR/UI"
filename_ps1n2_results = "ps1n2_results.txt"  # output text file of run results
filename_df = "df_ps1n2_results"  # output df of run results
filename_df_csv = filename_df + ".csv"
filename_output_progress = "_progress_df_ps1n2_results.txt"
# archive of df, add today's date to archive file name
filename_df_archive = filename_df + "_" + date_now
# columns to test for duplicate rows in 'df_ps1n2_results' before dropping
columns_drop_duplicate = [
    "date_end",
    "str_iloc_offsets",
    "slice_start",
    "slice_stop",
]

# print formats
print_format2 = "{:<30s}   {:>10s}   {:>20s}"
print_format3 = "{:<30s}   {:>10s}"
print_format4 = "{:<36s}{:>20s}{:>20s} %"
print_format5 = "{:<36s}{:>20s}"
print_format6 = "{:<20s}  {:<50s}\n"
print_format7 = "{:<20s}  {:<50s}"
# endregion constants

# region get XOM index date
col_names = ["date", "open", "high", "low", "close", "volume"]
path_XOM = my_path + index_symbol + ".csv"
# read XOM.csv, use 1st column "date" as index
df = pd.read_csv(
    path_symbols_data + index_symbol + ".csv",
    names=col_names,
    parse_dates=True,
    index_col=0,
)
date_index_all_dates = df.index

if verbose:
    print("path_XOM: {}".format(path_XOM))
    print(
        "date_index_all_dates({}):\n{}\n".format(
            type(date_index_all_dates), date_index_all_dates
        )
    )
# endregion get XOM index date

# region error check dates: throw error if date_start_limit is not in index
if date_start_limit is not None:
    if date_start_limit not in date_index_all_dates:
        error_msg_date_not_in_index = (
            "\ndate_start_limit {} NOT in date_index_all_dates: \n{}\n"
        )
        raise (
            ValueError(
                error_msg_date_not_in_index.format(
                    date_start_limit, date_index_all_dates
                )
            )
        )
# throw error if date_end_limit is not in index
if date_end_limit is not None:
    if date_end_limit not in date_index_all_dates:
        error_msg_date_not_in_index = (
            "\ndate_end_limit {} NOT in date_index_all_dates: \n{}\n"
        )
        raise (
            ValueError(
                error_msg_date_not_in_index.format(
                    date_end_limit, date_index_all_dates
                )
            )
        )
# endregion error check dates

# region update symbols' data
if force_update_df_symbols_close:  # force update
    print("force_update_df_symbols_close==True, update_symbols_data")
    (
        symbols_with_numeric_data_n_valid_dates,
        symbols_no_csv_data,
        symbols_duplicate_dates,
        df_symbols_close,
        df_symbols_volume,
    ) = update_symbols_data()
elif not os.path.isfile(path_data_dump + "df_symbols_close"):  # file missing
    print("file df_symbols_close DOES NOT exists, update_symbols_data")
    (
        symbols_with_numeric_data_n_valid_dates,
        symbols_no_csv_data,
        symbols_duplicate_dates,
        df_symbols_close,
        df_symbols_volume,
    ) = update_symbols_data()
else:  # check file is up to date
    # get file's last date
    date_last_df_symbols_close = (
        pd.read_pickle(path_data_dump + "df_symbols_close")
        .index[-1]
        .strftime("%Y-%m-%d")
    )
    # compare file's last date to requested date limit and index's last date
    if (
        date_last_df_symbols_close
        < date_end_limit
        <= date_index_all_dates[-1].strftime("%Y-%m-%d")
        # and is requested date limit in index
    ) and (date_end_limit in date_index_all_dates):
        print("df_symbols_close is not up to date, update_symbols_data")
        (
            symbols_with_numeric_data_n_valid_dates,
            symbols_no_csv_data,
            symbols_duplicate_dates,
            df_symbols_close,
            df_symbols_volume,
        ) = update_symbols_data()
    else:  # file is up to date, retrieve the data
        print("OHLCV data is up to date, retrieving OHLCV data")
        symbols_with_numeric_data_n_valid_dates = pickle_load(
            path_data_dump, "symbols_with_numeric_data_n_valid_dates"
        )
        symbols_no_csv_data = pickle_load(
            path_data_dump, "symbols_no_csv_data"
        )
        df_symbols_close = pd.read_pickle(path_data_dump + "df_symbols_close")
        df_symbols_volume = pd.read_pickle(
            path_data_dump + "df_symbols_volume"
        )
# print index, symbol
if verbose:
    print("symbols list")
    print("{:<7s}  :  {:10s}  ".format("index", "symbol"))
    # create index for list
    for i, symbol in enumerate(symbols_with_numeric_data_n_valid_dates):
        print("{:<7d}  :  {:10s}  ".format(i, symbol))
# endregion update symbols' data

# region get a list of end_dates to calculate performance stats
# get iloc of date_end_limit in XOM's date index
date_start, date_end, iloc_date_start, iloc_date_end = dates_within_limits(
    date_index_all_dates,
    date_start_limit,
    date_end_limit,
    iloc_offset=0,
    verbose=verbose,
)
iloc_end = iloc_date_end
# if iloc_iteration > 0, iloc_start is a list of end_dates
#  to iterate on symbols' performance
iloc_start = iloc_end - iloc_iteration
# list of end_dates to calculate performance stats
end_dates = date_index_all_dates[iloc_start : (iloc_end + 1)]
end_dates = end_dates.strftime("%Y-%m-%d")  # convert datatime to str
pickle_dump(end_dates, path_data_dump, "end_dates")
list_dump(end_dates, path_data_dump, "end_dates.txt")

if verbose:
    print(
        "date_end_limit({}): {}\n".format(type(date_end_limit), date_end_limit)
    )
    print("iloc_end({}):\n{}\n".format(type(iloc_end), iloc_end))
    print("iloc_start({}):\n{}\n".format(type(iloc_start), iloc_start))
    print("end_dates({}):\n{}\n".format(type(end_dates), end_dates))
# endregion get a list of end_dates to calculate performance stats

# region ++++ calculate symbols' performance
# output file to check running process
# truncate the file to zero length before opening
open(path_data_dump + filename_output_progress, "w")
# iteration order: end_dates, slices, iloc_offsets
for iloc_offsets, top_symbols_range in itertools.product(
    set_iloc_offsets, set_top_symbols_ranges
):
    # convert iloc_offsets to str for subsetting dataframe df_ps1n2_results,
    #  e.g. [18, 36] to '[18, 36]'
    str_iloc_offsets = "'" + str(iloc_offsets) + "'"

    if verbose:
        print(
            "str_iloc_offsets({}): {}".format(
                type(str_iloc_offsets), str_iloc_offsets
            )
        )
        print(f"iloc_offsets: {iloc_offsets}")

    # calculate performance stats for each end_dates,
    counter = 0  # progress counter for loop iterations
    # iterate from last to first
    for i, end_date in enumerate(end_dates[::-1], start=-len(end_dates)):
        if verbose:
            print("+-=" * 40)
            print(
                f"START: i: {i}, end_date: {end_date}, "
                f"iloc_offsets: {iloc_offsets}"
            )
            print("+-=" * 40, "\n")

        # calc_perf_stats(end_date, my_path)  # calculate performance stats
        # calc_perf_stats(end_date)  # calculate performance stats
        calc_perf_stats()  # calculate performance stats

        if verbose:
            print("+-=" * 40)
            print(
                f"END: i: {i}, end_date: {end_date}, "
                f"iloc_offsets: {iloc_offsets}"
            )
            print("+-=" * 40, "\n")

        # output file to check running process
        with open(path_data_dump + filename_output_progress, "a") as outfile:
            date_time = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(
                f"counter: {counter},  date_time: {date_time}\n"
                f"iloc_offsets: {iloc_offsets}\n"
                f"top_symbols_range: {top_symbols_range}\n"
                f"i: {i}, end_date: {end_date}\n",
                file=outfile,
            )
        counter += 1
# endregion ++++ calculate symbols' performance
# endregion main

# region splits df_ps1n2_results into df_ps1n2_results_stock
#  and df_ps1n2_results_etf
df_ps1n2_results_stock = pd.read_pickle(path_data_dump + "df_ps1n2_results")
df_ps1n2_results_etf = pd.read_pickle(path_data_dump + "df_ps1n2_results")

# region create list of all stock symbols and list of all etf symbols
# symbol data files downloaded from Vanguard stock screen or ETF.com
stock_cols = [
    "Symbol",
    "Company Name",
    "Market Cap ($M)",
    "Sector",
    "Industry",
]
# stock with market-cap over 2b from Vanguard
# df_temp = pd.read_csv(
#     path_symbols_file + "MktCap2b.csv", encoding="unicode_escape"
# )
# stock with top 1200 market-cap
df_temp = pd.read_csv(file_stocks_Top1200_MktCap, encoding="unicode_escape")
df_temp = df_temp[stock_cols]  # subset of selected columns
all_stock_symbols = df_temp["Symbol"].tolist()  # list of stock symbols
pickle_dump(all_stock_symbols, path_data_dump, "all_stock_symbols")
print('===> pickle_dump("all_stock_symbols")\n')

etf_cols = ["Symbol", "Fund Name", "Segments", "AUM", "Spread %"]
# largest 1200 ETF by AUM from etf.com
df_temp = pd.read_csv(file_etfs_Top1200_AUM, encoding="unicode_escape")
df_temp = df_temp[etf_cols]  # subset of selected columns
all_etf_symbols = df_temp["Symbol"].tolist()  # list of ETF symbols
pickle_dump(all_etf_symbols, path_data_dump, "all_etf_symbols")
print('===> pickle_dump("all_etf_symbols")\n')
# endregion create list for all stock symbols and list for all etf symbols

# region === create two dataframes from df_ps1n2_results ===
#  df with just stock symbols and df with just etf symbols
columns_ps1n2 = [
    "k1",
    "k2",
    "k3",
    "k4",
    "k5",
    "k6",
    "k7",
    "k8",
    "k9",
    "k10",
    "k11",
    "k12",
]
df_ps1n2_results_stock[columns_ps1n2] = df_ps1n2_results_stock[
    columns_ps1n2
].apply(pd_lists_intersection, a_list=all_stock_symbols, verbose=verbose)
df_ps1n2_results_etf[columns_ps1n2] = df_ps1n2_results_etf[
    columns_ps1n2
].apply(pd_lists_intersection, a_list=all_etf_symbols, verbose=verbose)
# endregion separate df_ps1n2_results into two dataframes:

# region write out dataframes to pickle files and csv files
df_ps1n2_results_stock.to_pickle(path_data_dump + "df_ps1n2_results_stock")
df_ps1n2_results_stock.to_csv(path_data_dump + "df_ps1n2_results_stock.csv")
print(
    "Wrote df_ps1n2_results_stock to:\n {}{}".format(
        path_data_dump, "df_ps1n2_results_stock.csv"
    )
)
df_ps1n2_results_etf.to_pickle(path_data_dump + "df_ps1n2_results_etf")
df_ps1n2_results_etf.to_csv(path_data_dump + "df_ps1n2_results_etf.csv")
print(
    "Wrote df_ps1n2_results_etf to:\n {}{}".format(
        path_data_dump, "df_ps1n2_results_etf.csv"
    )
)
# endregion write out dataframes to pickle files and csv files
# endregion splits df_ps1n2_results into df_ps1n2_results_stock
