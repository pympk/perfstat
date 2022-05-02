def candlestick(symbol, df, plot_chart=True):
    """Plot candlestick chart and returns:
        symbol, date, UI_MW_short[-1], UI_MW_long[-1],
        diff_UI_MW_short[-1], diff_UI_MW_long[-1]

    Args:
        symbol(string): symbol for the dataframe df
        df(dataframe): dataframe with date index
                       and columns open, high, low, close, volume
        plot_chart(bool): plots chart if True, defaut is True

    Return:
        symbol(str): stock symbol, e.g. 'AMZN'
        date(str): date, e.g. '2018-01-05'
        UI_MW_short[-1](float): last value of UI_MW_short
        UI_MW_long[-1](float): last value of UI_MW_long
        diff_UI_MW_short[-1](float): last value of diff_UI_MW_short
        diff_UI_MW_long[-1](float): last value of diff_UI_MW_long
    """

    # Reference:
    # https://www.kaggle.com/arrowsx/crypto-currency-a-plotly-sklearn-tutorial
    # http://mrjbq7.github.io/ta-lib/

    from plotly.offline import plot
    from util import symb_perf_stats, OBV_calc, UI_MW
    import plotly.graph_objs as go
    import talib as ta
    import numpy as np

    # **** IMPORTANT: COMMENT-OUT LINES INCLUDING THE BRACKETS {...} ****
    # if no lines are between the brackets, chart will be plotted compressed
    # to the right side and leaving empty spaces on the left side
    # ********

    # chart_path = 'C:/Users/ping/Desktop/plotly/'  # directory to store charts
    chart_path = 'C:/Users/ping/OneDrive/Desktop/plotly/'  # directory to store charts
    chart_name = symbol + '.html'

    close = df.close.values  # convert dataframe to numpy array

    # +++++++++ panel layout ++++++++++
    x_annotation = 0.06  # text x position
    y_annotation_gap = 0.022  # text y spacing

    # panels' y positions
    y80_top = .99  # spike, height 1
    y80_btm = .98
    y70_top = .98  # price, height 38
    y70_txt = .96  # price, text
    y70_btm = .60
    # gap of 1
    y60_top = .59  # volume, height 5
    # y60_txt = .57  # volume, text
    y60_btm = .54
    # gap of 1
    y50_top = .53  # drawdown, height 5
    y50_txt = .52  # drawdown, text
    y50_btm = .48
    # gap of 1
    y40_top = .47  # OBV, height 5
    y40_txt = .46  # OBV, text
    y40_btm = .42
    # gap of 1
    y30_top = .41  # top, height 13
    y30_txt = .40  # top, text
    y30_btm = .28
    # gap of 1
    y20_top = .27  # middle, height 13
    y20_txt = .26  # middle, text
    y20_btm = .14
    # gap of 1
    y10_top = .13  # bottom, height 13
    y10_txt = .12  # bottom, text
    y10_btm = .0

    # +++++++++ chart colors ++++++++++
    # http://colormind.io/template/paper-dashboard/#
    INCREASING_COLOR = '#17BECF'
    DECREASING_COLOR = '#7F7F7F'
    BBANDS_COLOR = '#BDBCBC'
    LIGHT_COLOR = '#0194A2'
    DARK_COLOR = '#015C65'
    DRAWDOWN_COLOR = '#5B5170'

    # ++++++++++ moving window sizes +++++++++++
    window_short = 15
    window_long = 30

    # ++++++++++ indicators +++++++++++
    # ++ spike line ++
    # With Toggle Spike Lines is on, gives vertical lines down the chart
    #   when cursor is at top of chart
    spike_line = np.zeros(len(df.index))  # create a trace with zeros
    spike_line_color = BBANDS_COLOR

    # ++ moving averages ++
    EMA_fast_period = 50  # moving average rolling window width
    EMA_fast_label = 'EMA' + str(EMA_fast_period)  # moving average column name
    EMA_fast_color = LIGHT_COLOR
    EMA_fast = ta.EMA(close, timeperiod=EMA_fast_period)

    EMA_slow_period = 200  # moving average rolling window width
    EMA_slow_label = 'EMA' + str(EMA_slow_period)  # moving average column name
    EMA_slow_color = DARK_COLOR
    EMA_slow = ta.EMA(close, timeperiod=EMA_slow_period)

    # ++ Bollinger Bands ++
    BBands_period = 20
    BBands_stdev = 2
    BBands_label = \
        'BBands(' + str(BBands_period) + ',' + str(BBands_stdev) + ')'
    BB_upper_color = BBANDS_COLOR
    BB_avg_color = BBANDS_COLOR
    BB_lower_color = BBANDS_COLOR
    BB_upper, BB_avg, BB_lower = ta.BBANDS(close,
                                           timeperiod=BBands_period,
                                           nbdevup=BBands_stdev,
                                           nbdevdn=BBands_stdev,
                                           matype=0)  # simple-moving-average

    # ++ On-Balance-Volume ++
    OBV_period = 10  # OBV's EMA period
    OBV, OBV_EMA, OBV_slope = OBV_calc(df, symbol, EMA_pd=OBV_period,
                                       tail_pd=5, norm_pd=30)

    # ++ On-Balance-Volume Difference ++
    OBV_diff = OBV - OBV_EMA

    # ++ UI M.W., Ulcer-Index Moving-Window ++
    # get drawdown array
    period_yr, CAGR, CAGR_Std, CAGR_UI, daily_return_std,  Std_UI, \
        drawdown, ulcer_index, max_drawdown = symb_perf_stats(close)
    # calculate Ulcer_Index of moving-window applied to drawdown
    UI_MW_short = UI_MW(drawdown, window=window_short)
    UI_MW_long = UI_MW(drawdown, window=window_long)

    # ++ Diff. UI M.W., difference in UI_MW_short and long arrays ++
    diff_UI_MW_short = np.diff(UI_MW_short)
    diff_UI_MW_long = np.diff(UI_MW_long)
    # num of leading NaN needed to pad arrays to the same length as 'close'
    num_NA_pad_short = len(close) - len(diff_UI_MW_short)
    num_NA_pad_long = len(close) - len(diff_UI_MW_long)
    # add leading NaN pad to the arrays
    diff_UI_MW_short = np.pad(diff_UI_MW_short, (num_NA_pad_short, 0),
                              'constant', constant_values=(np.nan))
    diff_UI_MW_long = np.pad(diff_UI_MW_long, (num_NA_pad_long, 0),
                             'constant', constant_values=(np.nan))

    # +++++ Cache +++++
    # change date index from 2018-09-04 00:00:00 to 2018-09-04
    #   dtype also change from datetime64[ns] to object
    date = df.index[-1].strftime("%Y-%m-%d")
    cache = (symbol, date, UI_MW_short[-1], UI_MW_long[-1],
             diff_UI_MW_short[-1], diff_UI_MW_long[-1])

    # +++++ Set Volume Bar Colors +++++
    colors_volume_bar = []

    for i, _ in enumerate(df.index):
        if i != 0:
            if df['close'].iloc[i] > df['close'].iloc[i-1]:
                colors_volume_bar.append(INCREASING_COLOR)
            else:
                colors_volume_bar.append(DECREASING_COLOR)
        else:
            colors_volume_bar.append(DECREASING_COLOR)

    # +++++ Set OBV_diff Bar Colors +++++
    colors_OBV_diff_bar = []

    for value in OBV_diff:
        if value > 0:
            colors_OBV_diff_bar.append(INCREASING_COLOR)
        else:
            colors_OBV_diff_bar.append(DECREASING_COLOR)

    # +++++++ panel definition ++++++++
    # ++ drawdown panel ++
    DD_title = 'DD'
    DD_label = DD_title
    DD_panel_text1 = 'UI: ' + str('%.3f' % ulcer_index)
    DD_panel_text2 = 'MaxDD: ' + str('%.3f' % max_drawdown)
    DD_panel_trace1 = drawdown
    DD_trace1_color = DRAWDOWN_COLOR
    DD_trace2_color = None

    # ++ OBV panel ++
    OBV_title = 'OBV'
    OBV_label = 'OBV'
    OBV_EMA_label = 'EMA' + str(OBV_period)
    OBV_panel_text1 = 'OBV Slope: ' + '%.3f' % OBV_slope
    OBV_panel_text2 = None  # NOQA
    OBV_panel_trace1 = OBV
    OBV_trace1_color = LIGHT_COLOR
    OBV_panel_trace2 = OBV_EMA
    OBV_trace2_color = DARK_COLOR

    # ++ top panel ++
    top_title = 'Diff. OBV'
    top_panel_text1 = OBV_label + '-' + OBV_EMA_label
    top_panel_trace1 = OBV_diff
    top_trace1_color = LIGHT_COLOR

    # ++ middle panel ++
    mid_title = 'UI MW'
    mid_panel_text1 = 'MW' + str(window_short)
    mid_panel_text2 = 'MW' + str(window_long)
    mid_panel_trace1 = UI_MW_short
    mid_panel_trace2 = UI_MW_long
    mid_trace1_color = LIGHT_COLOR
    mid_trace2_color = DARK_COLOR

    # ++ bottom panel ++
    btm_title = 'Diff. UI M.W.'
    btm_panel_text1 = 'MW' + str(window_short)
    btm_panel_text2 = 'MW' + str(window_long)
    btm_panel_trace1 = diff_UI_MW_short
    btm_panel_trace2 = diff_UI_MW_long
    btm_trace1_color = LIGHT_COLOR
    btm_trace2_color = DARK_COLOR

    # +++++++ plotly layout ++++++++
    layout = {
        # borders
        'margin': {
            't': 20,
            'b': 20,
            'r': 20,
            'l': 80,
        },
        'annotations': [
            # symbol
            {
                'x': 0.25,
                'y': 0.998,
                'showarrow': False,
                'text': '<b>' + symbol + '</b>',  # bold
                'xref': 'paper',
                'yref': 'paper',
                'xanchor': 'left',
                'yanchor': 'bottom',
                'font': {
                    'size': 15,  # larger font
                }
            },
            # period
            {
                'x': x_annotation,
                'y': y70_txt,  # 1st line
                'text': 'Years: ' + '%.2f' % period_yr,
                'xref': 'paper',
                'yref': 'paper',
                'xanchor': 'left',
                'yanchor': 'middle',
                'showarrow': True,
                'arrowhead': 0,
                'arrowwidth': 1,
                'arrowcolor': DECREASING_COLOR,
                'ax': -60,
                'ay': 0,
                'font': {
                    'size': 12,  # default font size is 12
                }
            },
            # Std(Daily Returns) / Ulcer_Index, Smaller is better
            {
                'x': x_annotation,
                'y': y70_txt - 1 * y_annotation_gap,  # 2nd line
                'text': 'Std/UI: ' + '%.3f' % Std_UI,
                'xref': 'paper',
                'yref': 'paper',
                'xanchor': 'left',
                'yanchor': 'middle',
                'showarrow': True,
                'arrowhead': 0,
                'arrowwidth': 1,
                'arrowcolor': DECREASING_COLOR,
                'ax': -60,
                'ay': 0,
                'font': {
                    'size': 12,  # default font size is 12
                }
            },
            # CAGR
            {
                'x': x_annotation,
                'y': y70_txt - 2 * y_annotation_gap,  # 3rd line
                'text': 'CAGR: ' + '%.3f' % CAGR,
                'xref': 'paper',
                'yref': 'paper',
                'xanchor': 'left',
                'yanchor': 'middle',
                'showarrow': True,
                'arrowhead': 0,
                'arrowwidth': 1,
                'arrowcolor': DECREASING_COLOR,
                'ax': -60,
                'ay': 0,
                'font': {
                    'size': 12,  # default font size is 12
                }
            },
            # CAGR / Std
            {
                'x': x_annotation,
                'y': y70_txt - 3 * y_annotation_gap,  # 4th line
                'text': 'CAGR/Std: ' + '%.3f' % CAGR_Std,
                'xref': 'paper',
                'yref': 'paper',
                'xanchor': 'left',
                'yanchor': 'middle',
                'showarrow': True,
                'arrowhead': 0,
                'arrowwidth': 1,
                'arrowcolor': DECREASING_COLOR,
                'ax': -60,
                'ay': 0,
                'font': {
                    'size': 12,  # default font size is 12
                }
            },
            # CAGR / UI
            {
                'x': x_annotation,
                'y': y70_txt - 4 * y_annotation_gap,  # 5th line
                'text': 'CAGR/UI: ' + '%.3f' % CAGR_UI,
                'xref': 'paper',
                'yref': 'paper',
                'xanchor': 'left',
                'yanchor': 'middle',
                'showarrow': True,
                'arrowhead': 0,
                'arrowwidth': 1,
                'arrowcolor': DECREASING_COLOR,
                'ax': -60,
                'ay': 0,
                'font': {
                    'size': 12,  # default font size is 12
                }
            },
            # drawdown panel text1 line
            {
                'x': x_annotation,
                'y': y50_txt,
                'text': DD_panel_text1,
                'xref': 'paper',
                'yref': 'paper',
                'xanchor': 'left',
                'yanchor': 'middle',
                'showarrow': True,
                'arrowhead': 0,
                'arrowwidth': 1,
                'arrowcolor': DD_trace1_color,
                'ax': -60,
                'ay': 0,
            },
            # drawdown panel text2 line
            {
                'x': x_annotation,
                'y': y50_txt - y_annotation_gap,
                'text': DD_panel_text2,
                'xref': 'paper',
                'yref': 'paper',
                'xanchor': 'left',
                'yanchor': 'middle',
                'showarrow': True,
                'arrowhead': 0,
                'arrowwidth': 1,
                'arrowcolor': DD_trace2_color,
                'ax': -60,
                'ay': 0,
            },
            # OBV panel text1 line
            {
                'x': x_annotation,
                'y': y40_txt,
                'text': OBV_panel_text1,
                'xref': 'paper',
                'yref': 'paper',
                'xanchor': 'left',
                'yanchor': 'middle',
                'showarrow': True,
                'arrowhead': 0,
                'arrowwidth': 1,
                'arrowcolor': OBV_trace1_color,
                'ax': -60,
                'ay': 0,
            },
            # top panel text1 line
            {
                'x': x_annotation,
                'y': y30_txt,
                'text': top_panel_text1,
                'xref': 'paper',
                'yref': 'paper',
                'xanchor': 'left',
                'yanchor': 'middle',
                'showarrow': True,
                'arrowhead': 0,
                'arrowwidth': 1,
                'arrowcolor': top_trace1_color,
                'ax': -60,
                'ay': 0,
            },
            # middle panel text1 line
            {
                'x': x_annotation,
                'y': y20_txt,
                'text': mid_panel_text1,
                'xref': 'paper',
                'yref': 'paper',
                'xanchor': 'left',
                'yanchor': 'middle',
                'showarrow': True,
                'arrowhead': 0,
                'arrowwidth': 1,
                'arrowcolor': mid_trace1_color,
                'ax': -60,
                'ay': 0,
            },
            # middle panel text2 line
            {
                'x': x_annotation,
                'y': y20_txt - y_annotation_gap,
                'text': mid_panel_text2,
                'xref': 'paper',
                'yref': 'paper',
                'xanchor': 'left',
                'yanchor': 'middle',
                'showarrow': True,
                'arrowhead': 0,
                'arrowwidth': 1,
                'arrowcolor': mid_trace2_color,
                'ax': -60,
                'ay': 0,
            },
            # bottom panel text1
            {
                'x': x_annotation,
                'y': y10_txt,
                'text': btm_panel_text1,
                'xref': 'paper',
                'yref': 'paper',
                'xanchor': 'left',
                'yanchor': 'middle',
                'showarrow': True,
                'arrowhead': 0,
                'arrowwidth': 1,
                'arrowcolor': btm_trace1_color,
                'ax': -60,
                'ay': 0,
            },
            # bottom panel text2
            {
                'x': x_annotation,
                'y': y10_txt - y_annotation_gap,
                'text': btm_panel_text2,
                'xref': 'paper',
                'yref': 'paper',
                'xanchor': 'left',
                'yanchor': 'middle',
                'showarrow': True,
                'arrowhead': 0,
                'arrowwidth': 1,
                'arrowcolor': btm_trace2_color,
                'ax': -60,
                'ay': 0,
            },
        ],
        'xaxis': {
            # range slider bar
            'rangeslider': {
                'visible': False  # removes rangeslider panel at bottom
            },
            # range buttons to interact
            'rangeselector': {
                'visible': True,
                'bgcolor': 'rgba(150, 200, 250, 0.4)',  # background color
                'x': 0,
                'y': 1,
                'buttons': [
                    {'count': 1, 'label': 'reset', 'step': 'all'},
                    {'count': 1, 'label': '1 yr', 'step': 'year',
                        'stepmode': 'backward'},
                    {'count': 6, 'label': '6 mo', 'step': 'month',
                        'stepmode': 'backward'},
                    {'count': 3, 'label': '3 mo', 'step': 'month',
                        'stepmode': 'backward'},
                    {'count': 1, 'label': '1 mo', 'step': 'month',
                        'stepmode': 'backward'},
                    {'count': 1, 'label': 'ytd', 'step': 'year',
                        'stepmode': 'todate'},
                ]
            }
        },
        # panel legends
        'legend': {
            'orientation': 'h',
            'y': 0.99, 'x': 0.3,
            'yanchor': 'bottom'
        },
        # panel spike line
        'yaxis80': {
            'domain': [y80_btm, y80_top],
            'showticklabels': False,
            'showline': False,
        },
        # panel price
        'yaxis70': {
            'domain': [y70_btm, y70_top],
            'showticklabels': True,
            'showline': True,
            'title': 'Price',
            'type': 'log',
        },
        # panel volume
        'yaxis60': {
            'domain': [y60_btm, y60_top],
            'showticklabels': True,
            'showline': True,
            'title': 'Vol.',
            'titlefont': {
                'size': 12
            },
            # 'type': 'log',
        },
        # panel drawdown
        'yaxis50': {
            'domain': [y50_btm, y50_top],
            'showticklabels': True,
            'showline': True,
            'title': DD_title,
            'titlefont': {
                'size': 12
            },
            # 'type': 'log',
        },
        # panel OBV
        'yaxis40': {
            'domain': [y40_btm, y40_top],
            'showticklabels': True,
            'showline': True,
            'title': OBV_title,
            'titlefont': {
                'size': 12
            },
            # 'type': 'log',
        },
        # panel top
        'yaxis30': {
            'domain': [y30_btm, y30_top],
            'showticklabels': True,
            'showline': True,  # vertical line on right side
            'title': top_title,
            'titlefont': {
                'size': 12
            },
            'tickmode': 'array',  # set ticklines per tickvals array
            # 'tickvals': [0, 25, 75, 100],  # ticklines
            # 'tickvals': [25, 75],  # ticklines
            # 'type': 'log',
        },
        # panel mid
        'yaxis20': {
            'domain': [y20_btm, y20_top],
            'showticklabels': True,
            'showline': True,
            'title': mid_title,
            'titlefont': {
                'size': 12
            },
            'tickmode': 'array',  # set ticklines per tickvals array
            # 'tickvals': [0, 25, 75, 100],  # ticklines
            # 'type': 'log',
        },
        # panel bottom
        'yaxis10': {
            'domain': [y10_btm, y10_top],
            'showticklabels': True,
            'showline': True,
            'title': btm_title,
            'titlefont': {
                'size': 12
            },
            # 'type': 'log',
            # 'tickmode': 'array',  # set ticklines per tickvals array
            # 'tickvals': [0, 25, 75, 100],  # ticklines
        },
    }

    # ++++++++++++ traces +++++++++++++
    data = []

    # spike line
    trace_spike_line = go.Scatter(
        # .astype(float) converts df.volume from integer to float for talib
        x=df.index,
        y=spike_line,
        yaxis='y80', line=dict(width=1),
        marker=dict(color=spike_line_color), hoverinfo='none',
        name='', showlegend=False,
    )
    data.append(trace_spike_line)

    # Candlestick Chart
    trace_price = go.Candlestick(
        x=df.index, open=df.open, high=df.high, low=df.low, close=df.close,
        yaxis='y70', name=symbol,
        increasing=dict(line=dict(color=INCREASING_COLOR)),
        decreasing=dict(line=dict(color=DECREASING_COLOR)),
        showlegend=False  # don't show legend across top of graph
    )
    data.append(trace_price)

    # Moving Averages
    trace_EMA_fast = go.Scatter(
        x=df.index,
        y=EMA_fast,
        yaxis='y70',
        name=EMA_fast_label,
        marker=dict(color=EMA_fast_color),
        hoverinfo='all',
        line=dict(width=1)
    )
    data.append(trace_EMA_fast)

    trace_EMA_slow = go.Scatter(
        x=df.index,
        y=EMA_slow,
        yaxis='y70',
        name=EMA_slow_label,
        marker=dict(color=EMA_slow_color),
        hoverinfo='all',
        line=dict(width=1)
    )
    data.append(trace_EMA_slow)

    # Bollinger Bands
    trace_BB_upper = go.Scatter(
        x=df.index, y=BB_upper,
        yaxis='y70', line=dict(width=1),
        marker=dict(color=BB_upper_color), hoverinfo='all',
        name=BBands_label,
        legendgroup='Bollinger Bands'
    )
    data.append(trace_BB_upper)

    trace_BB_avg = go.Scatter(
        x=df.index, y=BB_avg,
        yaxis='y70', line=dict(width=1),
        marker=dict(color=BB_avg_color), hoverinfo='all',
        name=BBands_label, showlegend=False,  # only show legend for upperband
        legendgroup='Bollinger Bands'
    )
    data.append(trace_BB_avg)

    trace_BB_lower = go.Scatter(
        x=df.index, y=BB_lower,
        yaxis='y70', line=dict(width=1),
        marker=dict(color=BB_lower_color), hoverinfo='all',
        name=BBands_label, showlegend=False,  # only show legend for upperband
        legendgroup='Bollinger Bands'
    )
    data.append(trace_BB_lower)

    # Volume Bars
    trace_vol = go.Bar(
        x=df.index, y=df.volume,
        marker=dict(color=colors_volume_bar),
        yaxis='y60', name='Volume',
        showlegend=False
    )
    data.append(trace_vol)

    # panel traces
    drawdown_trace1 = go.Bar(
        x=df.index, y=DD_panel_trace1,
        yaxis='y50',
        # line=dict(width=1),
        marker=dict(color=DD_trace1_color),
        # hoverinfo='all',
        name=DD_label, showlegend=False,
    )
    data.append(drawdown_trace1)

    OBV_trace1 = go.Scatter(
        x=df.index, y=OBV_panel_trace1,
        yaxis='y40',
        line=dict(width=1),
        marker=dict(color=OBV_trace1_color),
        # hoverinfo='all',
        name=OBV_label, showlegend=False,
    )
    data.append(OBV_trace1)

    OBV_trace2 = go.Scatter(
        x=df.index, y=OBV_panel_trace2,
        yaxis='y40',
        # line=dict(width=1),
        line=dict(width=1.2),  # darker line for trace2
        marker=dict(color=OBV_trace2_color),
        # hoverinfo='all',
        name=OBV_EMA_label, showlegend=False,
    )
    data.append(OBV_trace2)

    top_trace1 = go.Bar(
        x=df.index, y=top_panel_trace1,
        yaxis='y30',
        # line=dict(width=1),
        marker=dict(color=colors_OBV_diff_bar), hoverinfo='all',
        name=top_panel_text1, showlegend=False,
    )
    data.append(top_trace1)

    mid_trace1 = go.Scatter(
        x=df.index, y=mid_panel_trace1,
        yaxis='y20', line=dict(width=1),
        marker=dict(color=mid_trace1_color), hoverinfo='all',
        name=mid_panel_text1, showlegend=False,
    )
    data.append(mid_trace1)

    mid_trace2 = go.Scatter(
        x=df.index, y=mid_panel_trace2,
        # yaxis='y20', line=dict(width=1),
        yaxis='y20', line=dict(width=1.2),  # darker line for trace2
        marker=dict(color=mid_trace2_color), hoverinfo='all',
        name=mid_panel_text2, showlegend=False,
    )
    data.append(mid_trace2)

    btm_trace1 = go.Scatter(
        x=df.index, y=btm_panel_trace1,
        yaxis='y10', line=dict(width=1),
        marker=dict(color=btm_trace1_color), hoverinfo='all',
        name=btm_panel_text1, showlegend=False,
    )
    data.append(btm_trace1)

    btm_trace2 = go.Scatter(
        x=df.index, y=btm_panel_trace2,
        # yaxis='y10', line=dict(width=1),
        yaxis='y10', line=dict(width=1.2),  # darker line for trace2
        marker=dict(color=btm_trace2_color), hoverinfo='all',
        name=btm_panel_text2, showlegend=False,
    )
    data.append(btm_trace2)

    if plot_chart:
        fig = go.Figure(data=data, layout=layout)
        plot(fig, filename=chart_path+chart_name)

    return cache
