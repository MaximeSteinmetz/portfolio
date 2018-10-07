
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import pandas_market_calendars as mcal
from mpl_finance import candlestick2_ohlc
import matplotlib.ticker as ticker
import pytz
import datetime as dt


class Results:
    def __init__(self, strats, timespan_df):
        self.strats = strats
        self.trades = []
        data = np.zeros(len(timespan_df))
        self.vacant = pd.DataFrame({'Timestamp': timespan_df, 0: data})
        self.invested = pd.DataFrame({'Timestamp': timespan_df, 0: data})
        self.position = pd.DataFrame({'Timestamp': timespan_df, 0: data})
        for index, st in enumerate(strats):
            if index != 0:
                self.vacant[index] = self.vacant[0]
                self.invested[index] = self.invested[0]
                self.position[index] = self.position[0]
        self.vacant = self.vacant.set_index('Timestamp')
        self.invested = self.invested.set_index('Timestamp')
        self.position = self.position.set_index('Timestamp')

    def plot_equities(self, remove_overnight=False, settings=None):
        equities = self.invested.add(self.vacant, fill_value=0)
        if remove_overnight:
            equities = Utils.remove_overnight(equities, self.strats, 0, settings)
            quotes = equities.reset_index()
            idate = [i for i in range(len(quotes))]
            fig, ax = plt.subplots(1, 1)
            for i in range(len(self.strats)):
                ax.plot(idate, equities[i], label="Strategy " + str(i))
                plt.legend(loc='best')
                plt.title("Strategies equity curves without overnight")
        else:
            fig, ax = plt.subplots(1, 1)
            for i in range(len(self.strats)):
                plt.plot(equities[i], label="Strategy " + str(i))
            plt.legend(loc='best')
            plt.xticks(rotation=45)
            plt.title("Strategies equity curves")
        plt.show(block=False)

    def plot_total_equity(self, remove_overnight=False, settings = None):
        equities = self.invested.add(self.vacant, fill_value=0)
        total_equity = equities.iloc[:, :].sum(axis=1)
        total_equity = pd.DataFrame({'Timestamp': total_equity.index, 'Total equity': total_equity.values})
        total_equity = total_equity.set_index('Timestamp')

        if remove_overnight:
            label = "Total Equity"
            title = "Total equity curve without overnight"
            self.xticks_plot(total_equity, 0, label, title, settings)
        else:
            self.simple_plot(total_equity, "Total Equity", "Total equity curve")
        plt.show(block=False)

    def plot_strategy_equity(self, st_id, remove_overnight=False, settings=None):
        st_equity = self.invested[st_id].add(self.vacant[st_id], fill_value=0)
        if remove_overnight:
            label = "Strategy " + str(st_id) + " Equity"
            title = "Strategy " + str(st_id) + " Equity without overnight"
            self.xticks_plot(st_equity, st_id, label, title, settings)
        else:
            self.simple_plot(st_equity, "Strategy " + str(st_id) + " Equity", "Strategy " + str(st_id) + " Equity")
        plt.show(block=False)

    def plot_indicator(self, st_id, ind_id, remove_overnight=False, settings=None):
        ind = self.strats[st_id].ind[ind_id]
        if remove_overnight:
            label = ""
            title = self.strats[st_id].algos[ind_id].__repr__() + " for strategy " + str(st_id)
            self.xticks_plot(ind, st_id, label, title, settings)
        else:
            self.simple_plot(ind, "Strategy " + str(st_id) + " Equity", "Strategy " + str(st_id) + " Equity")
        plt.show(block=False)

    def plot_prices(self, st_id):
        fig, ax = plt.subplots(1, 1)
        quotes = self.strats[st_id].prices.tz_localize(pytz.timezone('UTC'))
        quotes = quotes.reset_index()
        candlestick2_ohlc(ax, quotes['open'], quotes['high'], quotes['low'], quotes['close'], width=0.4, colorup='g',
                          colordown='r', alpha=0.4)
        tdate = [i for i in quotes['Timestamp']]
        ax.xaxis.set_major_locator(ticker.MaxNLocator(10))
        ax.xaxis.set_minor_locator(ticker.MaxNLocator(40))

        for trade in self.trades:
            if trade.strategy_id == st_id:
                index = tdate.index(trade.timestamp)
                if trade.quantity > 0:
                    plt.plot(index, trade.price, 'go')
                elif trade.quantity < 0:
                    plt.plot(index, trade.price, 'ro')
        fig.autofmt_xdate()

        plt.title("Strategy " + str(st_id) + " prices")

        plt.show(block=False)

    def simple_plot(self, values, label, title):
        fig, ax = plt.subplots(1, 1)
        plt.plot(values, label=label)
        plt.legend(loc='best')
        plt.xticks(rotation=45)
        plt.title(title)

    def xticks_plot(self, values, st_id, label, title, s):
        st_equity = Utils.remove_overnight(values, self.strats, st_id, s)
        quotes = st_equity.reset_index()
        idate = [i for i in range(len(quotes))]
        fig, ax = plt.subplots(1, 1)
        ax.plot(idate, st_equity, label=label)
        plt.legend(loc='best')
        plt.title(title)

    def basic_metrics(self, st_id, s):
        st_equity = self.invested[st_id].add(self.vacant[st_id], fill_value=0)
        df = st_equity
        cal = self.strats[st_id].cal
        trades = self.trades

        ar, tr, asr = Utils.returns_sharpe(df, cal, s)
        bd, wd = Utils.best_worst_day(df, cal, s)
        tf = Utils.total_fees(st_id, trades)
        ag, al, rgl, bg, bl = Utils.gains_losses(trades, st_id)
        md, mdd, add = Utils.get_drawdown_stats(df, self.strats, st_id, s)
        se = st_equity[0]
        fe = st_equity[-1]

        print('Strategy ' + str(st_id) + ' results:')
        print('Annualized Sharpe Ratio: ' + '%.3f' % asr)
        print('Annual Return: ' + '%.3f' % ar + ' %')
        print('Total Return: ' + '%.3f' % tr + ' %')
        print('Best Day: ' + '%.3f' % bd + ' %')
        print('Worst Day: ' + '%.3f' % wd + ' %')
        print('Maximum Drawdown: ' + '%.3f' % md + ' %')
        print('Maximum Drawdown Duration: ' + '%.3f' % mdd)
        print('Average Drawdown Duration: ' + '%.3f' % add)
        print('Total Fees: ' + '%.3f' % tf)
        print('Average Gain: ' + '%.3f' % ag)
        print('Average Loss: ' + '%.3f' % al)
        print('Ratio Gain / Loss: ' + '%.3f' % rgl)
        print('Biggest Gain: ' + '%.3f' % bg)
        print('Biggest Loss: ' + '%.3f' % bl)
        print('Starting equity: ' + '%.3f' % se)
        print('Final equity: ' + '%.3f' % fe)
        print('---')

    def print_strategies(self):
        for st_id, st in enumerate(self.strats):
            print('Strategy ' + str(st_id) + ': ' + st.__repr__())

    def strategy_trades(self, st_id):
        print('Trades list of strategy ' + str(st_id) + ': ' + self.strats[st_id].__repr__())
        for trade in self.trades:
            if trade.strategy_id == st_id:
                print('Quantity: ' + str(trade.quantity) + ', Price: ' + str(trade.price) +
                      ', Timestamp: ' + trade.timestamp.strftime("%Y-%m-%d %H:%M:%S") + ', Fees: %.3f' % trade.fees)

    def total_trades(self):
        for trade in self.trades:
            print('Strategy ' + str(trade.strategy_id) + ', Quantity: ' + str(trade.quantity) + ', Price: ' + str(trade.price) +
                      ', Timestamp: ' + trade.timestamp.strftime("%Y-%m-%d %H:%M:%S") + ', Fees: %.3f' % trade.fees)

    def update_trade(self, trade):
        self.trades.append(trade)

    def update_bar(self, dashboard, strats, bar):
        for index, st in enumerate(strats):
            self.vacant[index][bar] = dashboard.vacant[index]
            self.invested[index][bar] = dashboard.invested[index]
            self.position[index][bar] = dashboard.position[index]


class Utils:

    @classmethod
    def gains_losses(cls, trades, st_id):
        gains = []
        losses = []
        trade_count = 0
        gain_loss = 0
        for trade in trades:
            if trade.strategy_id == st_id:
                trade_count += 1
                gain_loss += -trade.value - trade.fees
                if trade_count == 2:
                    if gain_loss >= 0:
                        gains.append(gain_loss)
                        trade_count = 0
                        gain_loss = 0
                    if gain_loss < 0:
                        losses.append(gain_loss)
                        trade_count = 0
                        gain_loss = 0

        if len(gains) == 0:
            ag = 0
            bg = 0
        else:
            ag = sum(gains) / len(gains)
            bg = max(gains)
        if len(losses) == 0:
            al = 0
            bl = 0
        else:
            al = sum(losses) / len(losses)
            bl = min(losses)
        if len(losses) + len(gains) == 0:
            rgl = 0
        else:
            rgl = len(gains) / (len(losses) + len(gains))

        return ag, al, rgl, bg, bl

    @classmethod
    def total_fees(cls, st_id, trades):
        tf = 0
        for trade in trades:
            if trade.strategy_id == st_id:
                tf += trade.fees
        return tf

    @classmethod
    def best_worst_day(cls, df, cal, s):
        start = df.index[0]
        end = df.index[-1]
        if not s.instr == 'crypto':
            schedule = cal.schedule(start, end)
        else:
            return 0, 0
        return_days = []
        for day in range(len(schedule)):
            try:
                ts = schedule['market_open'][day]
                start_day = df[ts]
            except KeyError:
                ts = s.start_dt_utc
                start_day = df[ts]

            try:
                ts = schedule['market_close'][day]
                end_day = df[ts]
            except KeyError:
                ts = s.end_dt_utc
                end_day = df[ts]
            return_days.append(end_day / start_day - 1)
        bd = max(return_days) * 100
        wd = min(return_days) * 100
        return bd, wd

    @classmethod
    def returns_sharpe(cls, df, cal, s):
        returns = []
        ts = [i for i in df.index]
        for i in range(len(ts))[:-1]:
            ret_high_res = df[ts[i + 1]] / df[ts[i]] - 1
            returns.append(ret_high_res)
        if not s.instr == 'crypto':
            start = df.index[0]
            schedule = cal.schedule(start, start)
            dt_index = mcal.date_range(schedule, s.freq, dtype='datetime64', closed='left')
            trading_pts_day = len(dt_index)
        else:
            trading_pts_day = 24 * 3600 / s.freq_seconds()

        mean = np.mean(returns)
        std = np.std(returns)

        ann_mean = (1 + mean) ** (trading_pts_day * 252) - 1
        ann_std = std * np.sqrt(trading_pts_day * 252)

        asr = (ann_mean - s.risk_free_rate) / ann_std
        ar = ann_mean * 100
        tr = (np.prod([1 + i for i in returns]) - 1) * 100

        return ar, tr, asr

    @classmethod
    def get_drawdown_stats(cls, df, strats, st_id, s):
        drawdowns = []
        drawdowns_start = []
        dd_start = 0
        capturing_dd = False
        if s.intraday and not s.instr == 'crypto':
            df = cls.remove_overnight(df, strats, st_id, s)
            df = df[st_id]
        for i in range(1, len(df)):
            if df[i] >= df[i - 1] and not capturing_dd:
                dd_start = i
            elif df[i] < df[dd_start]:
                if not capturing_dd:
                    capturing_dd = True
                    drawdown = [df[i - 1]]
                    drawdowns_start.append(dd_start)
                drawdown.append(df[i])
            elif df[i] >= df[dd_start] and capturing_dd:
                drawdown.append(df[i])
                drawdowns.append(drawdown)
                capturing_dd = False
        if capturing_dd:
            drawdowns.append(drawdown)

        percentage_drawdowns = []
        for drawdown in drawdowns:
            first_val = drawdown[0]
            percentage_drawdown = []
            for value in drawdown:
                percentage_drawdown.append(value / first_val - 1)
            percentage_drawdowns.append(percentage_drawdown)

        md = min(min(percentage_drawdowns, key=min)) * 100
        mdd = max(map(len, percentage_drawdowns)) - 2
        add = 0
        for dd_list in percentage_drawdowns:
            add += len(dd_list)
        add /= len(percentage_drawdowns)
        return md, mdd, add

    @classmethod
    def remove_overnight(cls, values, strats, st_id, s):
        cal = strats[st_id].cal
        schedule = cal.schedule(values.index[0], values.index[-1])
        dt_index = mcal.date_range(schedule, s.freq, dtype='datetime64', closed='left')
        df = pd.DataFrame(dt_index)
        df.columns = ['Timestamp']
        mask1 = (df['Timestamp'] >= values.index[0])
        mask2 = (values.index[-1] >= df['Timestamp'])
        df = df.loc[mask1]
        df = df.loc[mask2]
        values = values.reset_index()
        values = df.merge(values, how='left', on='Timestamp')
        values = values.set_index('Timestamp')
        return values