import pandas as pd


# ++++ Set directory ++++
path_symbols_data = \
    'C:/Users/ping/Google Drive/stocks/vanguard_commission_free_etfs/equities/'  # NOQA
# path_symbols_data = \
#     'C:/Users/ping/Google Drive/stocks/vanguard_commission_free_etfs/bonds/'  # NOQA
# path_symbols_data = \
#     'C:/Users/ping/Google Drive/stocks/sp500_n_nasdaq100_vanguard_mid400/'  # NOQA

path_data_dump = path_symbols_data + 'VSCode_dump/'

# ++++ read UI_MW and diff_UI_MW files into dataframes
df_UI_MW_short = pd.read_csv(path_data_dump + 'df_UI_MW_short.csv',
                             index_col=0)
df_UI_MW_long = pd.read_csv(path_data_dump + 'df_UI_MW_long.csv', index_col=0)
df_diff_UI_MW_short = pd.read_csv(path_data_dump + 'df_diff_UI_MW_short.csv',
                                  index_col=0)
df_diff_UI_MW_long = pd.read_csv(path_data_dump + 'df_diff_UI_MW_long.csv',
                                 index_col=0)


# ++++ create dataframe to store the last value
#   of Ulcer-Index-Moving-Windows and their row differences.
#   Symbol is the column name, index is Date.
#   Column values are symbol's last values in the respective dataframes.
df = None  # create empty dataframe
df_temp = df_UI_MW_short[-1:]  # get the value of the last row
df = pd.concat([df, df_temp], axis=0)  # append value to df
df_temp = df_UI_MW_long[-1:]
df = pd.concat([df, df_temp], axis=0)
df_temp = df_diff_UI_MW_short[-1:]
df = pd.concat([df, df_temp], axis=0)
df_temp = df_diff_UI_MW_long[-1:]
df = pd.concat([df, df_temp], axis=0)
print('\n', '+'*20)
print('df.head(): ', '\n', df.head())
print('df.index.name: ', df.index.name)


# ++++ Transpose df. Index is symbol names. Column names are the last date.
print('\n', '+'*20)
df_n_diff_UI_MW_ends = df.T  # transpose dataframe
print('df_n_diff_UI_MW_ends.tail(): ', '\n', df_n_diff_UI_MW_ends.tail())
print('\n', '+'*20)
print('df_n_diff_UI_MW_ends.columns.names: ',
      df_n_diff_UI_MW_ends.columns.names)
print('df_n_diff_UI_MW_ends.index.name: ', df_n_diff_UI_MW_ends.index.name)
print('df_n_diff_UI_MW_ends.index.values: ', df_n_diff_UI_MW_ends.index.values)


# ++++ change index name to 'symbol', column name to respective df name
#  column names to the last data date.
df_n_diff_UI_MW_ends.index.name = 'symbol'
df_n_diff_UI_MW_ends.columns = ['UI_MW_short', 'UI_MW_long',
                                'diff_UI_MW_short', 'diff_UI_MW_long']
date_last_row = 'Date: ' + str(df.index.values[0])  # date of last row
df_n_diff_UI_MW_ends.columns.names = [date_last_row]
print('\n', '+'*20)
print('df_n_diff_UI_MW_ends.head(): ', '\n', df_n_diff_UI_MW_ends.head())
print('\n', '+'*20)
print('df_n_diff_UI_MW_ends.tail(): ', '\n', df_n_diff_UI_MW_ends.tail(), '\n')


# ++++ write df to file, and read it back
df_n_diff_UI_MW_ends.to_csv(path_data_dump + 'df_n_diff_UI_MW_ends.csv')
print('Wrote df_n_diff_UI_MW_ends.csv to {}'.format(path_data_dump), '\n')
