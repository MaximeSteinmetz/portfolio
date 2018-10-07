from price_uploader import PUBase as PU
from strategy import Strategy
import pandas as pd
from results import Results
import pytz
import datetime as dt
import numpy as np
from backtester import Backtester as BT


class Portfolio:
    """
    Models a portfolio for strategies and contains the methods for performing backtests
    """
    def __init__(self, strat_dicts, s):
        self.strats = []
        for strat_dict in strat_dicts:
            for symbol in strat_dict['symbols']:
                self.strats.append(Strategy(strat_dict['algos'], symbol))
        for st in self.strats:
            if not s.instr == 'crypto':
                st.cal = st.get_cal()
            st.bia = st.get_bia()
            st.capital = s.initial_capital / len(self.strats)
        self.dashboard = Dashboard(self.strats)

    def timespan(self):
        starts = []
        ends = []
        for st in self.strats:
            prices = st.prices
            start = prices.index[0]
            end = prices.index[-1]
            starts.append(start)
            ends.append(end)
        earliest_start = min(starts)
        latest_end = max(ends)
        earliest_start = earliest_start.replace(tzinfo=pytz.timezone('UTC'))
        latest_end = latest_end.replace(tzinfo=pytz.timezone('UTC'))
        return [earliest_start, latest_end]

    def bt(self, s):
        """
        Main method for backtesting.
        :return: Results object
        """

        print('Launching backtest')

        # Initialization
        for st in self.strats:
            st.prices = PU.get_prices(st, s)
            st.ind, st.signals = st.get_ind_and_signals()
        [earliest_start, latest_end] = self.timespan()
        bt_timespan = pd.date_range(s.start_dt_utc, latest_end, freq=s.freq)
        results = Results(self.strats, bt_timespan)
        schedules = []

        if s.instr != 'crypto':
            for st in self.strats:
                schedules.append(st.cal.schedule(earliest_start, latest_end))

        print('Backtest started!')
        now = dt.datetime.now()

        # Signal algos
        for st in self.strats:
            for algo in s.signal_algos:
                st.signals = algo.treat_signal(st, s)


        # Proper backtest bars
        for bar in bt_timespan:
            if s.lb.do_lb:
                self.lb_bt()
            for st_index, st in enumerate(self.strats):
                signal = 0
                try:
                    signal = st.signals[bar]
                except KeyError:
                    signal = 0
                finally:
                    trade = self.get_trade(signal, bar, st_index, s)
                    if trade != "No trade":
                        self.dashboard.update_trade(trade)
                        results.update_trade(trade)
            self.dashboard.update_bar(self.strats, bar)
            results.update_bar(self.dashboard, self.strats, bar)

        time = dt.datetime.now() - now
        print('Backtest performed. Computation  time: ' + str(time))
        print('Backtest contained ' + str(len(self.strats)) + ' strategies.')

        return results

    def get_trade(self, signal, bar, st_index, s):
        sizes = []
        for sizing_algo in s.sizing_algos:
            size = sizing_algo.process_signal(signal, bar, st_index, self.strats, self.dashboard)
            sizes.append(size)
        size = min(sizes)
        if size != 0:  # critical line for SRD management
            st = self.strats[st_index]
            price = st.prices['close'][bar]  # all lines with 'close' critical for "buy at next open" option
            fees = s.broker.get_fees(size, price)
            trade = Trade(size, price, bar, fees, st.symbol, st_index)
            return trade
        else:
            return "No trade"

    def lb_bt(self):
        pass


class Dashboard:
    def __init__(self, strats):
        self.position = []
        self.invested = []
        self.vacant = []
        for index, st in enumerate(strats):
            self.position.append(0)
            self.invested.append(0)
            self.vacant.append(st.capital)

    def update_trade(self, trade):
        index = trade.strategy_id
        self.position[index] += trade.quantity
        self.invested[index] += trade.value
        self.vacant[index] += -trade.value - trade.fees

    def update_bar(self, strats, bar):
        for index, st in enumerate(strats):
            try:
                self.invested[index] = self.position[index] * st.prices['close'][bar]
            except KeyError:
                pass


class Trade:
    def __init__(self, quantity, price, timestamp, fees, symbol, strategy_id):
        self.quantity = quantity
        self.price = price
        self.timestamp = timestamp
        self.fees = fees
        self.symbol = symbol
        self.strategy_id = strategy_id

    def __repr__(self):
        return 'Trade: ' + self.symbol + ', ' + str(self.quantity) + ', ' + str(self.price) + ', ' + \
               str(self.timestamp) + ', fees: ' + str(self.fees) + ', strategy ' + str(self.strategy_id)

    @property
    def value(self):
        return self.quantity * self.price
