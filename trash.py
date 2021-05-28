import pandas as pd
from util import pickle_dump, pickle_load

##########
# GEAGY l_close[1140] has duplicate dates on 2021-04-08
##########

path_temp = 'C:/Users/ping/Google Drive/stocks/MktCap2b_AUMtop1200/VSCode_dump/'
l_close = pickle_load(path_temp, 'trash_l_close')
#########
# l_close_1 = l_close[1140]  # err GEAGY
#########

l_close_1 = l_close[1139:1141]  #



print(len(l_close_1))
# print(l_close_1)
df = pd.concat(l_close_1, axis=1)
# print(df)
