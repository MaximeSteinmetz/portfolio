from sql import SQL
import calendars as cals
import pandas_market_calendars as mcal


class Strategy:
    def __init__(self, algos, symbol):
        self.algos = algos
        self.symbol = symbol
        self.prices = "No price data assigned"
        self.signals = "No signal data assigned"
        self.cal = "No calendar assigned"
        self.bia = "No bars in advance value assigned"
        self.ind = "No indicators data assigned"
        self.capital = "No capital allowance assigned"

    def __repr__(self):
        algo_str = ''
        for algo in self.algos:
            algo_str += algo.__repr__() + ', '
        algo_str = algo_str[:-2]
        return algo_str + ' with symbol ' + self.symbol

    def get_cal(self):
        ex_name = SQL.get_exchange_name(self.symbol)
        ex_mcal = cals.get_mcal_exchange(ex_name)
        cal = mcal.get_calendar(ex_mcal)
        return cal

    def get_bia(self):
        bias = []
        for algo in self.algos:
            bia = algo.bia
            bias.append(bia)
        return max(bias)

    def get_ind_and_signals(self):
        signals = []
        inds = []
        for algo in self.algos:
            ind, signal = algo.get_ind_and_signals(self.prices)
            inds.append(ind)
            signals.append(signal)
        # Merge all signals into one
        if len(signals) > 1:
            nb_iter = len(signals) - 1
            for i in range(nb_iter):
                signals[1][signals[0] == -1] = -1
                del signals[0]
        return inds, signals[0]
