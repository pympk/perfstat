import datetime as dt
from dateutil.relativedelta import relativedelta
from ulcer_index import ulcer_index
from util import get_symbol_data_from_dir
# from util import Timer


# ++++ Set start date and end date ++++
# date_start = '2017-09-09'  # yyyy-mm-dd, yyyy-mm, yyyy?
date_start = dt.date.today() - relativedelta(years=1, months=0, days=0)
# convert date_start from class 'datetime.date' to 'str'
#   to prevent KeyError: datetime.date(2017, 9, 9)
date_start = date_start.strftime('%Y-%m-%d')
date_end = None  # yyyy-mm-dd, yyyy-mm, yyyy, None

print('date_start: ', date_start, type(date_start))
print('date_end:   ', date_end, type(date_end))
print('date_today: ', dt.date.today(), type(dt.date.today()), '\n')

# symbol data directory
data_path = 'C:/Users/ping/Desktop/td_etf_data/sp500-1/'

# load symbol data into dataframes
dfs, df_row_count, symbols = \
    get_symbol_data_from_dir(data_path, file_ext='.csv',
                             date_start=date_start, date_end=date_end)

symbol = symbols[0]
# symbol = 'SKYY'
df = dfs[symbol]
print('df:', symbol)
print(df, '\n')

close = df.close.values  # convert to numpy.ndarray
drawdown, ulcer_index, max_drawdown = ulcer_index(close)

print('\n')
print('drawdown[:5] = ', '\n', drawdown[:5], '\n')
print('ulcer_index = ', ulcer_index, '\n')
print('max_drawdown = ', max_drawdown, '\n')
print('len(close) = ', len(close))
print('len(drawdown) = ', len(drawdown), '\n')
