def ulcer_index(close):
    """Return drawdown and ulcer_index using stock's close price
       https://stackoverflow.com/questions/36750571/calculate-max-draw-down-with-a-vectorized-solution-in-python
       http://www.tangotools.com/ui/ui.htm
       Calculation CHECKED against: http://www.tangotools.com/ui/UlcerIndex.xls

    Args:
        close(numpy.ndarray): stock price at the close

    Return:
        drawdown(numpy.ndarray): drawdown from peak, 0.05 means 5% drawdown
        ulcer_index(float64): UI
        max_drawdown(float64): maximum drawdown
    """

    import numpy as np

    first_close = close[0]
    close_count = len(close)

    cum_return = close / first_close
    # calculate percent drawdown
    drawdown = -1 * (1 - cum_return / np.maximum.accumulate(cum_return))
    max_drawdown = drawdown.min()
    drawdown_square = np.square(drawdown)
    sum_drawdown_square = np.sum(drawdown_square)
    UI = np.sqrt(sum_drawdown_square / close_count)

    return drawdown, UI, max_drawdown
