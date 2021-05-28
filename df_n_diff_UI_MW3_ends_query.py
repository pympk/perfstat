import pandas as pd


# Set directories and file name
# path_symbols_data = \
#     'C:/Users/ping/Google Drive/stocks/vanguard_commission_free_etfs/equities/'  # NOQA
# path_symbols_data = \
#     'C:/Users/ping/Google Drive/stocks/vanguard_commission_free_etfs/bonds/'  # NOQA
# path_symbols_data = \
#     'C:/Users/ping/Google Drive/stocks/sp500_n_nasdaq100_vanguard_mid400/'  # NOQA
path_symbols_data = \
    'C:/Users/ping/Google Drive/stocks/MktCap2b_AUMtop1200/'

path_data_dump = path_symbols_data + 'VSCode_dump/'
file_path_df_UI_MW_ends = path_data_dump + 'df_n_diff_UI_MW_ends.csv'
# read files back into dataframe
df = pd.read_csv(file_path_df_UI_MW_ends, index_col=0)

# ++++ query df
# query 'diff_UI_MW_short <= 0 & diff_UI_MW_long <= 0'
df_diff_short_and_long_negative = \
    df.query('diff_UI_MW_short <= 0 & diff_UI_MW_long <= 0')
# convert df's index values, whhich are symbols, into a list
list_df_diff_short_and_long_negative =\
      list(df_diff_short_and_long_negative.index.values)
print('list_df_diff_short_and_long_negative:\n{}\n'
      .format(list_df_diff_short_and_long_negative))

# query 'diff_UI_MW_short <= 0'
df_diff_short_negative = df.query('diff_UI_MW_short <= 0')
print('df_diff_short_negative:\n{}\n'.format(df_diff_short_negative))
print('type(df_diff_short_negative):\n{}\n'
      .format(type(df_diff_short_negative)))
# convert df's index values, whhich are symbols, into a list
list_df_diff_short_negative = \
      list(df_diff_short_negative.index.values)
print('list_df_diff_short_negative:\n{}\n'
      .format(list_df_diff_short_negative))

# get difference between the two lists, convert the result set to list
long_positive_with_short_negative = \
      list(set(list_df_diff_short_negative)
           .difference(list_df_diff_short_and_long_negative))
print('long_positive_with_short_negative:\n{}\n'
      .format(long_positive_with_short_negative))
