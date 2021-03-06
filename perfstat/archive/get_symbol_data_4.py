def get_symbol_data_4(
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
        symbols_with_csv_data[str]: list of symbols in file_symobls with csv
            data, e.g. ['AMZN', 'QQQ', 'SPY', ...]
        symbols_no_csv_data[str]: list of symbols in file_symobls without csv
            data, e.g. ['AMZN', 'QQQ', 'SPY', ...]
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

    # put index_symbol in fist position, i.e. symbols[0]
    try:
        index = symbols.index(index_symbol)
        symbols.insert(0, symbols.pop(index))
    except ValueError:  # index_symbol not in symbols
        symbols.insert(0, index_symbol)
    # endregion get list of symbols

    # column names of .csv files
    col_names = ["date", "open", "high", "low", "close", "volume"]

    symbols_no_csv_data = []
    l_close = []  # list to store symbols' close
    l_volume = []  # list to store symbols' volume
    dfs = {}  # dictionary of dataframe for symbol data: date, open,...
    for i, symbol in enumerate(symbols):
        if i % 100 == 0:  # print every 100 symbols
            print(f'index: {i}  symbol: {symbol}')
        try:  # if symbol csv file exists
            df = pd.read_csv(
                dir_path + symbol + ".csv",
                names=col_names,
                parse_dates=True,
                index_col=0,
            )
            if df.index.isnull().any():  # check for NaN in index
                # index has NaN, symbol PRN index has many NaN
                symbols_no_csv_data.append(symbol)
            else:
                # index had no NaN
                dfs.update({symbol: df})
                close = df.close[date_start:date_end]
                close.rename(symbol, inplace=True)
                l_close.append(close)
                volume = df.volume[date_start:date_end]
                volume.rename(symbol, inplace=True)
                l_volume.append(volume)
        except Exception:  # no such symbol csv file
            symbols_no_csv_data.append(symbol)

    df_symbols_close = pd.concat(l_close, axis=1)
    df_symbols_volume = pd.concat(l_volume, axis=1)
    symbols_with_csv_data = list(set(symbols) - set(symbols_no_csv_data))
    print("{}\n".format("-" * 78))
    gc.collect()

    return (
        dfs,
        symbols_with_csv_data,
        symbols_no_csv_data,
        df_symbols_close,
        df_symbols_volume,
    )
