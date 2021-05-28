import datetime as dt  # NOQA
from util import plot_symbols, print_symbol_data, pickle_dump


# symbols = ['FTEC', 'WCLD', 'SMH', 'ICLN', 'CNRG']  # symbols to plot
symbols = ['BTC-USD', 'ETH-USD', 'UNI3-USD', 'LINK-USD', 'MATIC-USD']  # symbols to plot

# last date of data
date_end_limit = '2021-05-27'
# date_end_limit = dt.date.today().strftime("%Y-%m-%d")

# switches to print and plot symbols data
print_syms = True
# print_syms = False
plot_syms = True
# plot_syms = False

iloc_offset = 252  # number of days to plot
rows_OHLCV_display = 6  # number of rows of OHLCV to display

# directory path of VSCode_dump
path_symbols_data = (
    "C:/Users/ping/Google Drive/stocks/MktCap2b_AUMtop1200/"  # NOQA
)
path_data_dump = path_symbols_data + "VSCode_dump/"

# plot symbols of the last iloc_offset trading days
if plot_syms:
    plot_symbols(
        symbols=symbols,
        path_data_dump=path_data_dump,
        date_start_limit=None,
        date_end_limit=date_end_limit,
        iloc_offset=iloc_offset,
    )
# print symbols data
if print_syms:
    print_symbol_data(
        symbols=symbols,
        path_symbols_data=path_symbols_data,
        date_end_limit=date_end_limit,
        rows_OHLCV_display=rows_OHLCV_display,
    )
