def ulcer_index_vectorized(df_symbols_close):
    """Return drawdown, ulcer-index and max-drawdown of stocks' close prices
       https://stackoverflow.com/questions/36750571/calculate-max-draw-down-with-a-vectorized-solution-in-python
       http://www.tangotools.com/ui/ui.htm
       Calculation CHECKED against: http://www.tangotools.com/ui/UlcerIndex.xls
       Calculation VERIFIED in: ulcer_index_check.ipynb

    Args:
        df_symbols_close(dataframe): dataframe with date as index,
          symbol's close as columns, and symbol as column name.

    Return:
        drawdown(dataframe): drawdown from peak, 0.05 means 5% drawdown
        UI(pandas series float64): ulcer-index
        max_drawdown(pandas series float64): maximum drawdown
    """

    import numpy as np

    symbols = df_symbols_close.columns
    df_symbols_returns = df_symbols_close / df_symbols_close.shift(1) - 1
    df_symbols_returns_std = df_symbols_returns.std(ddof=1)
    # +++ SET RETURNS OF FIRST ROW = 0,
    #  otherwise drawdown calculation starts with the second row
    df_symbols_returns.iloc[0] = 0
    cum_returns = (1 + df_symbols_returns).cumprod()
    drawdown = cum_returns.div(cum_returns.cummax()) - 1
    max_drawdown = drawdown.min()
    len_drawdown = len(drawdown)
    UI = np.sqrt(np.sum(np.square(drawdown)) / len_drawdown)
    Std_UI = df_symbols_returns_std / UI
    period_yr = len(df_symbols_close) / 252  # 252 trading days per year
    CAGR = (df_symbols_close.iloc[-1] / df_symbols_close.iloc[0]) \
        ** (1 / period_yr) - 1
    CAGR_Std = CAGR / df_symbols_returns_std
    CAGR_UI = CAGR / UI

    return symbols, period_yr, drawdown, UI, max_drawdown, \
        df_symbols_returns_std, Std_UI, CAGR, CAGR_Std, CAGR_UI
