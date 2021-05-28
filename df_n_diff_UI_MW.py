from util import UI_MW, get_df_symbols_close
from ulcer_index import ulcer_index
from dateutil.relativedelta import relativedelta
import datetime as dt
import pandas as pd


# ++++ Set directories and file name ++++
# path_symbols_data = 'C:/Users/ping/Desktop/td_etf_data/~py_watch_list/'
# path_symbols_data = 'C:/Users/ping/Desktop/td_etf_data/sp500_n_nasdaq100/'
# path_symbols_data = 'C:/Users/ping/Desktop/td_etf_data/~sp500_n_nasdaq100_vanguard_mid400/'  # NOQA
path_symbols_data = \
    'C:/Users/ping/Desktop/New folder/'
    # 'C:/Users/ping/Google Drive/stocks/td_etf_data/~sp500_n_nasdaq100_vanguard_mid400/'  # NOQA
# path_data_dump = 'C:/Users/ping/Desktop/td_etf_data/VSCode_dump/'
path_data_dump = path_symbols_data + 'VSCode_dump/'
file_name_df_close = 'df_symbols_close.csv'


# ++++ Set start date and end date ++++
# date_start = '2017-09-09'  # yyyy-mm-dd, yyyy-mm, yyyy, None
# need minimum of 1 month and 13 days of data to generate diff_UI_MW_30
date_start = dt.date.today() - relativedelta(years=1, months=0, days=0)
# convert date_start from class 'datetime.date' to 'str'
#   to prevent KeyError: datetime.date(2017, 9, 9)
date_start = date_start.strftime('%Y-%m-%d')
date_end = None  # yyyy-mm-dd, yyyy-mm, yyyy, None


# ++++ Set moving-windows ++++
window_short = 15
window_long = 30


# ++++ gether symbols' close into a dataframe ++++
df_symbols_close = get_df_symbols_close(path_data_dump, file_name_df_close,
                                        path_symbols_data)
# df_symbols_close.to_csv(path_data_dump + 'df_symbols_close.csv')
# print('Wrote df_symbols_close.csv to {}'.format(path_data_dump))
print('df_symbols_close.head(): ', '\n', df_symbols_close.head(), '\n')
# print('df_symbols_close.tail(): ', '\n', df_symbols_close.tail(), '\n')
print('df_symbols_close.tail(20): ', '\n', df_symbols_close.tail(20), '\n')
df_symbols_close.to_csv(path_data_dump + 'df_symbols_close.csv')


# ++++ select date range ++++
df_symbols_close = df_symbols_close[date_start:date_end]

# ++++ drop columns (i.e. symbols) end with NaN from dataframe ++++
# find columns end with NaN and convert the column names to a list
columns_end_with_NaN = df_symbols_close.columns[df_symbols_close[-6:]
                                                .isna().any()].tolist()
# column names are symbols
print("Symbols with NaN as closing price: ", columns_end_with_NaN, '\n')
# drop columns (i.e. symbols) end with NaN
df_symbols_close = df_symbols_close.drop(columns=columns_end_with_NaN)
print('Dropped columns: {} from df_symbols_close.\n\
These columns end with NaN.'.format(columns_end_with_NaN), '\n')

# ++++ drop rows with 2 or more NaN from dataframe ++++
df_symbols_close_dropna = df_symbols_close.dropna(thresh=2)
index_difference = \
    df_symbols_close.index.difference(df_symbols_close_dropna.index).values
df_symbols_close = df_symbols_close_dropna
print('Dropped index {} from df_symbols_close.\n\
These rows have at least 2 NaN values'.format(index_difference), '\n')


# ++++ calculate symbol's drawdown and gether them into a dataframe ++++
df_drawdown = None  # create an empty dataframe
# calculate symbol's drawdown and gether them into a dataframe
for symbol in df_symbols_close:
    df_temp_close = df_symbols_close[symbol]  # df with symbol's close
    # calculate Ulcer-Index using using df_temp_close with NaN rows dropped
    drawdown, UI, max_drawdown = ulcer_index(df_temp_close.dropna())
    # convert drawdown pandas.Series to pandas.Dateframe
    df_temp_DD = drawdown.to_frame()
    # append symbol's drawdown as column to df_drawdown
    df_drawdown = pd.concat([df_drawdown, df_temp_DD], axis=1)
print('df_drawdown.head(): ', '\n', df_drawdown.head(), '\n')
print('df_drawdown.tail(): ', '\n', df_drawdown.tail(), '\n')

# ++++ calculate Ulcer-Index of moving-window applied to symbol's close ++++
# create empty dataframe using index from df_drawdown
df_UI_MW_short = pd.DataFrame(index=df_drawdown.index)
print('\n', '+'*20)
# append dataframe with Ulcer-Index of moving-window applied to symbol's close
for symbol in df_drawdown:
    print('calculate Ulcer-Index-Moving-Window-Size {} for {}'
          .format(window_short, symbol))
    arr_UI_MW = UI_MW(df_drawdown[symbol], window=window_short)
    df_UI_MW_short[symbol] = arr_UI_MW  # append array to dataframe

# create empty dataframe with index from df_drawdown
df_UI_MW_long = pd.DataFrame(index=df_drawdown.index)
print('\n', '+'*20)
# append dataframe with Ulcer-Index of moving-window applied to symbol's close
for symbol in df_drawdown:
    print('calculate Ulcer-Index-Moving-Window-Size {} for {}'
          .format(window_long, symbol))
    arr_UI_MW = UI_MW(df_drawdown[symbol], window=window_long)
    df_UI_MW_long[symbol] = arr_UI_MW  # append array to dataframe


# ++++ create dataframes for row difference ++++
df_diff_UI_MW_short = df_UI_MW_short.diff()
# create dataframe of row difference
df_diff_UI_MW_long = df_UI_MW_long.diff()


# ++++ write dataframes to files ++++
print('\n', '+'*20)
df_UI_MW_short.to_csv(path_data_dump + 'df_UI_MW_short.csv')
print('Wrote df_UI_MW_short.csv to {}'.format(path_data_dump))
df_UI_MW_long.to_csv(path_data_dump + 'df_UI_MW_long.csv')
print('Wrote df_UI_MW_long.csv to {}'.format(path_data_dump))
df_diff_UI_MW_short.to_csv(path_data_dump + 'df_diff_UI_MW_short.csv')
print('Wrote df_diff_UI_MW_short.csv to {}'.format(path_data_dump))
df_diff_UI_MW_long.to_csv(path_data_dump + 'df_diff_UI_MW_long.csv')
print('Wrote df_diff_UI_MW_long.csv to {}'.format(path_data_dump))
