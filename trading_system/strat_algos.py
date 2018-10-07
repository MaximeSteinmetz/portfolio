
from abc import ABC, abstractmethod
import indicators as ind
import pandas as pd
import pytz


class StrategyAlgo(ABC):
    """
    Abstract base class for algo objects
    """

    @abstractmethod
    def get_ind_and_signals(self, prices):
        pass


class AboveMA(StrategyAlgo):
    def __init__(self, short_period, long_period):
        super(AboveMA, self).__init__()
        self.name = 'Above Moving Average'
        self.short_period = short_period
        self.long_period = long_period
        self.bia = long_period

    def __repr__(self):
        return 'AboveMA(' + str(self.short_period) + ', ' + str(self.long_period) + ')'

    def get_ind_and_signals(self, prices):
        short_MA = ind.moving_average(prices['close'], self.short_period)
        long_MA = ind.moving_average(prices['close'], self.long_period)
        signal = long_MA.copy()
        signal[short_MA > long_MA] = 1
        signal[short_MA < long_MA] = -1
        signal[short_MA == long_MA] = 0
        signal[long_MA.isnull()] = 0

        indicator = pd.DataFrame({'Timestamp': signal.index, 'short_MA': short_MA, 'long_MA': long_MA})
        indicator = indicator.set_index('Timestamp')
        indicator = indicator.tz_localize(pytz.timezone('UTC'))

        return indicator, signal


class RSI(StrategyAlgo):
    def __init__(self, n, high_value, low_value):
        super(RSI, self).__init__()
        self.name = 'RSI'
        self.n = n
        self.high_value = high_value
        self.low_value = low_value
        self.bia = n

    def __repr__(self):
        return 'RSI(' + str(self.n) + ', ' + str(self.high_value) + ', ' + str(self.low_value) + ')'

    def get_ind_and_signals(self, prices):
        rsi = ind.rsi(prices['close'], self.n)
        signal = rsi.copy()
        signal[:] = 0

        for i in range(1, len(rsi)):
            if rsi[i] >= self.low_value and rsi[i - 1] < self.low_value:
                signal[i] = 1
            if rsi[i] <= self.high_value and rsi[i - 1] > self.high_value:
                signal[i] = -1

        indicator = pd.DataFrame({'Timestamp': signal.index, 'rsi': rsi})
        indicator = indicator.set_index('Timestamp')
        indicator = indicator.tz_localize(pytz.timezone('UTC'))

        return indicator, signal


class Canal(StrategyAlgo):
    def __init__(self, bars_window, slope, height, stop_loss):
        super(Canal, self).__init__()
        self.name = 'Canal'
        self.bars_window = bars_window
        self.slope = slope
        self.height = height
        self.stop_loss = stop_loss
        self.bia = bars_window - 1

    def __repr__(self):
        return 'Canal(' + str(self.bars_window) + ', ' + str(self.slope) + ', ' + str(self.height) + ', ' + str(self.stop_loss) + ')'

    def get_ind_and_signals(self, prices):
        signal = prices['close'].copy()
        signal[:] = 0
        indicator = None
        bought = False
        lows = prices['low']
        for index in range(self.bars_window - 1, len(prices), 1):
            if not bought:
                first_low = lows[index - self.bars_window + 1]
                next_lows = [l for l in lows.iloc[index - self.bars_window + 2:index + 1]]
                if any(nl <= first_low for nl in next_lows):
                    pass
                elif not any(nl <= first_low for nl in next_lows):
                    slope = None
                    for id, l in enumerate(next_lows):
                        bar_dist = id + 1
                        new_slope = (l - first_low) / bar_dist
                        if slope is None:
                            slope = new_slope
                        elif new_slope < slope:
                            slope = new_slope
                    if slope >= self.slope:
                        signal[index] = 1
                        bought = True
                        bought_id = index
            elif bought:
                bar_dist = index - bought_id
                if lows[index] <= (slope * bar_dist) * (1 - self.stop_loss):
                    bought = False
                    signal[index] = -1

        return indicator, signal


class BuyAndHold(StrategyAlgo):
    def __init__(self):
        self.name = 'Buy and hold'
        self.bia = 0

    def __repr__(self):
        return 'Buy and hold'

    def get_ind_and_signals(self, prices):
        signal = pd.Series(1, index=prices.index)
        indicator = None

        return indicator, signal