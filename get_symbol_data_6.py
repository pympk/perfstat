def get_symbol_data_6(
    dir_path,
    file_symbols,
    date_start=None,
    date_end=None,
    index_symbol="XOM",
    verbose=False,
):
    """The function reads symbols in file_symbols and writes their OHLCV data
    into dataframes and store the dataframes to dictionary dfs. Dates are
    inclusive. None returns all data in csv file.

    The function create df_symbols_close, a dataframe with index_symbol's
    date as index, symbols' closing price in columns, and symbols as column
    names. Missing prices are filled with NaN.

    The function also create df_symbols_volume, a dataframe with index_symbol's
    date as index, symbols' daily volume in columns, and symbols as column
    names. Missing prices are filled with NaN.

    v6 screen out symbols with invalid dates and non-numeric data,
       added symbols_invalid_dates[] to store symbols with invalid dates
       added symbols_not_numeric[] to store symbols with non-numeric data
       renamed symbols_with_csv_data_n_unique_dates to
               symbols_with_numeric_data_n_valid_dates
    v5 check for duplicate date index, replace symbols_with_csv_data with
       symbols_with_numeric_data_n_valid_dates, return symbols_duplicate_dates
    v4 add 'if df.index.isnull().any():' to check for NaN in index,
       symbol PRN has NaN in index
    v1 add back index_symbol to symbols_with_csv_data

    Args:
        dir_path(str): directory path of the symbol data
        file_symbols(str): path + file-name to symbols-text-file
        date_start(str): 'yyyy-mm-dd', 'yyyy', 'yyyy-mm', None
        date_end(st): 'yyyy-mm-dd', 'yyyy', 'yyyy-mm', None
        index_symbol(str): date index for df_symbols_close, 'XOM'
        verbose(True or False): prints output if True, default False

    Return:
        dfs(dict}: dictionary of symbols' OHLCV dataframes with
            all available dates, e.g. dfs['QQQ']
        symbols_with_numeric_data_n_valid_dates[str]: list of symbols in
            file_symbols with csv data and unique date index
            data, e.g. ['AMZN', 'QQQ', 'SPY', ...]
        symbols_no_csv_data[str]: list of symbols in file_symbols without csv
            data, e.g. ['AMZN', 'QQQ', 'SPY', ...]
        symbols_duplicate_dates[str]: list of symbols in file_symbols
            without duplicate dates, e.g. ['AMZN', 'QQQ', 'SPY', ...]
        df_symbols_close(df): Dataframe with index_symbol's date as index,
            symbols' closing price in columns, and symbols as column names.
            Missing prices are filled with NaN.
        df_symbols_volume(df): Dataframe with index_symbol's date as index,
            symbols' daily volume in columns, and symbols as column names.
            Missing volume are filled with NaN.
    """

    import pandas as pd
    import gc

    print("+" * 29 + "  get_symbol_data  " + "+" * 29 + "\n")
    # region get list of symbols
    with open(file_symbols, "r") as f:  # get symbols from text file
        # removes leading and trailing whitespaces from the string
        symbols = [line.strip() for line in f]

    if verbose:
        print("symbols: {}\n".format(symbols))
        print(
            "len(symbols) before removing '' from symbol text file: {}".format(
                len(symbols)
            )
        )
    # removes '' in list of symbols, a blank line in text file makes '' in list
    symbols = list(filter(None, symbols))
    if verbose:
        print(
            f"len(symbols) after removing '' "
            f"from symbol text file: {len(symbols)}"
        )

    # put index_symbol in symbols[0]
    try:
        index = symbols.index(index_symbol)
        symbols.insert(0, symbols.pop(index))
    except ValueError:  # index_symbol not in symbols
        symbols.insert(0, index_symbol)
    # endregion get list of symbols

    # column names of .csv files
    col_names = ["date", "open", "high", "low", "close", "volume"]

    symbols_no_csv_data = []  # symbols with missing .csv file
    symbols_duplicate_dates = []  # symbols with duplicate dates
    symbols_invalid_dates = []  # symbols with invalid dates
    symbols_not_numeric = []  # symbols with non-numeric data
    l_close = []  # list to store symbols' close
    l_volume = []  # list to store symbols' volume
    dfs = {}  # dictionary of dataframe for symbol data: date, open,...

    for i, symbol in enumerate(symbols):
        if i % 100 == 0:  # print every 100 symbols
            print(f"index: {i}\tsymbol: {symbol}")
        try:  # if symbol csv file exists
            df = pd.read_csv(
                dir_path + symbol + ".csv",
                names=col_names,
                parse_dates=True,
                index_col=0,
            )
            # https://stackoverflow.com/questions/49435438/pandas-validate-date-format
            # return True if dates in df.index can be converted to pd.Timestamp objects
            if (
                pd.to_datetime(df.index, format="YYY-mm-dd", errors="coerce")
                .notnull()
                .all()
            ):
                # all valid dates in symbol's index
                if verbose:
                    print(f"{symbol:12} index dates are all valid dates")
                if df.index.has_duplicates:
                    if verbose:
                        print(f"{symbol:12} index dates have duplicates")
                    symbols_duplicate_dates.append(symbol)
                else:  # date index has no duplicates and has all valid dates
                    if verbose:
                        print(
                            f"{symbol:12} index dates are all valid dates, no duplicates"
                        )
                    # https://stackoverflow.com/questions/54426845/how-to-check-if-a-pandas-dataframe-contains-only-numeric-column-wise
                    # check items in df are all numeric and not empty
                    df_numeric = df.apply(
                        lambda s: pd.to_numeric(s, errors="coerce")
                        .notnull()
                        .all()
                    )
                    if pd.Series(
                        df_numeric
                    ).all():  # True if all items in df are numeric
                        # write df columns
                        dfs.update({symbol: df})
                        close = df.close[date_start:date_end]
                        close.rename(symbol, inplace=True)
                        l_close.append(close)
                        volume = df.volume[date_start:date_end]
                        volume.rename(symbol, inplace=True)
                        l_volume.append(volume)
                    else:  # df has non-numeric items
                        if verbose:
                            print(
                                f"{symbol:12} df has non-numeric items or empty cell(s)"
                            )
                        symbols_not_numeric.append(symbol)
            else:  # symbol's dates index are NOT valid dates
                symbols_invalid_dates.append(symbol)
        except FileNotFoundError:  # no such symbol csv file
            symbols_no_csv_data.append(symbol)

    df_symbols_close = pd.concat(l_close, axis=1)
    df_symbols_volume = pd.concat(l_volume, axis=1)
    symbols_with_numeric_data_n_valid_dates = list(
        set(symbols)
        - set(symbols_no_csv_data)
        - set(symbols_duplicate_dates)
        - set(symbols_invalid_dates)
        - set(symbols_not_numeric)
    )

    print("{}\n".format("-" * 78))
    gc.collect()

    return (
        dfs,
        symbols_with_numeric_data_n_valid_dates,
        symbols_no_csv_data,
        symbols_duplicate_dates,
        df_symbols_close,
        df_symbols_volume,
    )
