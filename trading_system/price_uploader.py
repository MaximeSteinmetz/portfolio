from abc import ABC, abstractmethod
from sql import SQL
import sql
import pandas_market_calendars as mcal
import datetime as dt
import pandas as pd
import pytz


class PUBase(ABC):

    @classmethod
    def get_prices(cls, st, s):
        pu = cls.get_pu_instance(s)
        start_utc = pu.get_start_bia(st, s)
        end_utc = s.end_dt_utc
        time_df = pu.get_time_df(st, start_utc, end_utc, s)
        raw_price_df = cls.get_price_df(st, start_utc, end_utc, s, pu)
        clean_price_df = pu.merge_and_fill(st, raw_price_df, time_df, s)
        return clean_price_df

    @classmethod
    def get_pu_instance(cls, s):
        """
        :return: The PriceUploader child instance for the needed upload request type
        """
        if s.instr == 'crypto':
            return Crypto()
        elif s.intraday:
            return PUIntraday()
        elif not s.intraday:
            return PUNotIntraday()

    @staticmethod
    def get_time_df(st, start, end, s):
        """
        Determines the symbol's trading hours and creates a dataframe containing all the trading points between start
        with lookback and end.
        :return: Timestamp dataframe containing all required time points
        """
        schedule = st.cal.schedule(start, end)
        dt_index = mcal.date_range(schedule, s.freq)
        df = pd.DataFrame(dt_index)
        df.columns = ['Timestamp']
        mask1 = (df['Timestamp'] >= start)
        mask2 = (end >= df['Timestamp'])
        df = df.loc[mask1]
        df = df.loc[mask2]
        return df

    @classmethod
    def get_price_df(cls, st, start, end, s, pu):
        select, where = pu.get_query_string(start, end, st.symbol, s)
        raw_data = SQL.get_price_data(select, where, s.freq)
        df = pu.raw_data_to_df(raw_data)
        return df

    @abstractmethod
    def merge_and_fill(self, st, raw_price_df, time_df, s):
        pass

    @abstractmethod
    def raw_data_to_df(self, raw_data):
        pass

    @abstractmethod
    def get_start_bia(self, st, s):
        pass

    @abstractmethod
    def get_query_string(self, start, end, symbol, s):
        pass


class Crypto(PUBase):
    def get_start_bia(self, st, s):
        start = s.start_dt_utc
        start += dt.timedelta(seconds=-s.freq_seconds() * st.bia)
        return start

    @staticmethod
    def get_time_df(st, start, end, s):
        """
        Determines the symbol's trading hours and creates a dataframe containing all the trading points between start
        with lookback and end.
        :return: Timestamp dataframe containing all required time points
        """
        dt_index = pd.DatetimeIndex(freq=s.freq, start=start, end=end)
        df = pd.DataFrame(dt_index)
        df.columns = ['Timestamp']
        return df

    @classmethod
    def get_query_string(cls, start, end, symbol, s):
        [price_date, ticker, open, high, low, close, volume] = [sql.price_date, sql.ticker, sql.open, sql.high,
                                                                sql.low, sql.close, sql.volume]

        select = cls.get_select_string(price_date, ticker, open, high, low, close, volume)
        where = cls.get_where_string(price_date, ticker, start, end, symbol)
        return [select, where]

    @classmethod
    def get_select_string(cls, price_date, ticker, open, high, low, close, volume):
        select = price_date + ', ' + ticker + ', ' + open + ', ' + high + ', ' + low + ', ' + close + ', ' + volume
        return select

    @classmethod
    def get_where_string(cls, price_date, ticker, start, end, symbol):
        start_str = start.strftime('%Y-%m-%d %H:%M:%S')
        end_str = end.strftime('%Y-%m-%d %H:%M:%S')
        where = ''
        where += price_date + ' >= "' + start_str + '" AND ' \
                 + price_date + ' <= "' + end_str + '" AND ('
        where += ticker + ' = "' + symbol + '"' + ' OR '
        where = where[:-4]
        where += ')'
        return where

    @classmethod
    def raw_data_to_df(cls, raw_data):
        """
        :param raw_data: Price data as extracted from SQL database (tuple)
        :return: pandas dataframe containing the price data
        """
        output_dict = {}
        [dates, open, high, low, close, volume] = [[item[0] for item in raw_data],
                                                   [item[2] for item in raw_data],
                                                   [item[3] for item in raw_data],
                                                   [item[4] for item in raw_data],
                                                   [item[5] for item in raw_data],
                                                   [item[6] for item in raw_data]]
        output_dict['Timestamp'] = dates
        output_dict['open'] = open
        output_dict['high'] = high
        output_dict['low'] = low
        output_dict['close'] = close
        output_dict['volume'] = volume
        df = pd.DataFrame(output_dict)
        return df

    @classmethod
    def merge_and_fill(cls, st, df, time_df, s):
        """
        :param df: The price dataframe, containing data holes
        :param time_df: The timestamp dataframe containing all the needed time points
        :return: A price dataframe with dataholes forward filled
        """
        # These two rows needed to convert datetimes from aware to naive
        time_df['Timestamp'] = time_df['Timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        time_df['Timestamp'] = pd.to_datetime(time_df['Timestamp'])

        comp_df = time_df.merge(df, how='left', on='Timestamp')
        nan_count = comp_df.isnull().sum().sum()
        cells_count = len(comp_df) * 5  # 5 = number of columns OHLC+Volume
        nan_perc = nan_count / cells_count * 100
        if nan_perc >= s.nan_perc_limit and st.symbol not in s.accepted_nan:
            print('WARNING: NaN percentage of ' + st.symbol + ' over the limit: %.3f' % nan_perc + ' %')
            input("Press Enter to proceed...")
            s.accepted_nan.append(st.symbol)

        comp_df = comp_df.ffill()
        comp_df = comp_df.fillna(method='backfill')
        comp_df = comp_df.set_index('Timestamp')
        comp_df = comp_df.astype(float)
        return comp_df


class PUIntraday(PUBase):

    @classmethod
    def merge_and_fill(cls, st,  df, time_df, s):
        """
        :param df: The price dataframe, containing data holes
        :param time_df: The timestamp dataframe containing all the needed time points
        :return: A price dataframe with dataholes forward filled
        """
        # These two rows needed to convert datetimes from aware to naive
        time_df['Timestamp'] = time_df['Timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        time_df['Timestamp'] = pd.to_datetime(time_df['Timestamp'])

        comp_df = time_df.merge(df, how='left', on='Timestamp')
        nan_count = comp_df.isnull().sum().sum()
        cells_count = len(comp_df) * 5 # 5 = number of columns OHLC+Volume
        nan_perc = nan_count / cells_count * 100
        if nan_perc >= s.nan_perc_limit and st.symbol not in s.accepted_nan:
            print('WARNING: NaN percentage of ' + st.symbol + ' over the limit: %.3f' % nan_perc + ' %')
            input("Press Enter to proceed...")
            s.accepted_nan.append(st.symbol)

        comp_df = comp_df.ffill()
        comp_df = comp_df.fillna(method='backfill')
        comp_df = comp_df.set_index('Timestamp')
        comp_df = comp_df.astype(float)
        return comp_df

    @classmethod
    def raw_data_to_df(cls, raw_data):
        """
        :param raw_data: Price data as extracted from SQL database (tuple)
        :return: pandas dataframe containing the price data
        """
        output_dict = {}
        [dates, open, high, low, close, volume] = [[item[0] for item in raw_data],
                                                         [item[2] for item in raw_data],
                                                         [item[3] for item in raw_data],
                                                         [item[4] for item in raw_data],
                                                         [item[5] for item in raw_data],
                                                         [item[6] for item in raw_data]]
        output_dict['Timestamp'] = dates
        output_dict['open'] = open
        output_dict['high'] = high
        output_dict['low'] = low
        output_dict['close'] = close
        output_dict['volume'] = volume
        df = pd.DataFrame(output_dict)
        return df

    @classmethod
    def get_query_string(cls, start, end, symbol, s):
        [price_date, ticker, open, high, low, close, volume] = [sql.price_date, sql.ticker, sql.open, sql.high,
                                                                sql.low, sql.close, sql.volume]

        select = cls.get_select_string(price_date, ticker, open, high, low, close, volume)
        where = cls.get_where_string(price_date, ticker, start, end, symbol)
        return [select, where]

    @classmethod
    def get_select_string(cls, price_date, ticker, open, high, low, close, volume):
        select = price_date + ', ' + ticker + ', ' + open + ', ' + high + ', ' + low + ', ' + close + ', ' + volume
        return select

    @classmethod
    def get_where_string(cls, price_date, ticker, start, end, symbol):
        start_str = start.strftime('%Y-%m-%d %H:%M:%S')
        end_str = end.strftime('%Y-%m-%d %H:%M:%S')
        where = ''
        where += price_date + ' >= "' + start_str + '" AND ' \
                 + price_date + ' <= "' + end_str + '" AND ('
        where += ticker + ' = "' + symbol + '"' + ' OR '
        where = where[:-4]
        where += ')'
        return where

    def get_start_bia(self, st, s):
        """
        :param lookback_pts: The number of lookback points required by a strategy
        :param s: settings
        :return: - The new start_dt in utc, with the required lookback points (if available)
                 - The number of lookback points that have been collected (notice if missed some)
        """
        if not self.start_in_trading_time(st.cal, s):
            return s.start_dt_utc
        elif self.start_in_trading_time(st.cal, s):
            nb_pts = self.get_nb_pts_to_last_open(st.cal, s)
            if nb_pts > st.bia:
                start = self.increment_start_backwards(st.bia, s)
                return start
            elif nb_pts <= st.bia:
                start = self.get_last_open(st.cal, s)
                return start

    def start_in_trading_time(self, cal, s):
        schedule = cal.schedule(s.start_dt_utc, s.start_dt_utc)
        if schedule.empty:
            return False
        open = schedule['market_open'][0]
        close = schedule['market_close'][0]
        if s.start_dt_utc > open and s.start_dt_utc >= close:
            return False
        elif s.start_dt_utc >= open and close > s.start_dt_utc:
            return True
        elif open > s.start_dt_utc and close >= s.start_dt_utc:
            return False

    def get_nb_pts_to_last_open(self, cal, s):
        schedule = cal.schedule(s.start_dt_utc, s.start_dt_utc)
        open = schedule['market_open'][0]
        diff = s.start_dt_utc - open
        minutes = (diff.components[0] * 1440) + (diff.components[1] * 60) + diff.components[2]
        diff_increment = int(minutes / s.freq_integer)
        return diff_increment

    def increment_start_backwards(self, bia, s):
        start_lookback_utc = s.start_dt_utc - dt.timedelta(0, bia * s.freq_integer * 60)
        return start_lookback_utc

    def get_last_open(self, cal, s):
        schedule = cal.schedule(s.start_dt_utc, s.start_dt_utc)
        open = schedule['market_open'][0]
        return open


class PUNotIntraday(PUBase):
    def get_start_bia(self, st, s):
        start = s.start_dt_utc
        schedule = st.cal.schedule(start, start)
        days_in_advance = mcal.date_range(schedule, '1d')
        while len(days_in_advance) < st.bia:
            start += dt.timedelta(days=-1)
            schedule = st.cal.schedule(start, s.start_dt_utc)
            days_in_advance = mcal.date_range(schedule, '1d')
        return start

    @classmethod
    def merge_and_fill(cls, st, df, time_df, s):
        """
        :param df: The price dataframe, containing data holes
        :param time_df: The timestamp dataframe containing all the needed time points
        :return: A price dataframe with dataholes forward filled
        """
        # These two rows needed to convert datetimes from aware to naive
        time_df['Timestamp'] = time_df['Timestamp'].dt.strftime('%Y-%m-%d')
        time_df['Timestamp'] = pd.to_datetime(time_df['Timestamp'])

        comp_df = time_df.merge(df, how='left', on='Timestamp')
        nan_count = comp_df.isnull().sum().sum()
        cells_count = len(comp_df) * 5  # 5 = number of columns OHLC+Volume
        nan_perc = nan_count / cells_count * 100
        if nan_perc >= s.nan_perc_limit and st.symbol not in s.accepted_nan:
            print('WARNING: NaN percentage of ' + st.symbol + ' over the limit: %.3f' % nan_perc + ' %')
            input("Press Enter to proceed...")
            s.accepted_nan.append(st.symbol)

        comp_df = comp_df.ffill()
        comp_df = comp_df.fillna(method='backfill')
        comp_df = comp_df.set_index('Timestamp')
        comp_df = comp_df.astype(float)
        return comp_df

    @classmethod
    def raw_data_to_df(cls, raw_data):
        """
        :param raw_data: Price data as extracted from SQL database (tuple)
        :return: pandas dataframe containing the price data
        """
        output_dict = {}
        [dates, open, high, low, close, adj_close, volume] = [[item[0] for item in raw_data],
                                                   [item[2] for item in raw_data],
                                                   [item[3] for item in raw_data],
                                                   [item[4] for item in raw_data],
                                                   [item[5] for item in raw_data],
                                                   [item[6] for item in raw_data],
                                                   [item[7] for item in raw_data]]
        output_dict['Timestamp'] = dates
        output_dict['open'] = open
        output_dict['high'] = high
        output_dict['low'] = low
        output_dict['close'] = close
        output_dict['adjusted_close'] = adj_close
        output_dict['volume'] = volume
        df = pd.DataFrame(output_dict)
        return df

    @classmethod
    def get_query_string(cls, start, end, symbol, s):
        [price_date, ticker, open, high, low, close, adj_close, volume] = [sql.price_date, sql.ticker, sql.open, sql.high,
                                                                sql.low, sql.close, sql.adj_close, sql.volume]

        select = cls.get_select_string(price_date, ticker, open, high, low, close, adj_close, volume)
        where = cls.get_where_string(price_date, ticker, start, end, symbol)
        return [select, where]

    @classmethod
    def get_select_string(cls, price_date, ticker, open, high, low, close, adj_close, volume):
        select = price_date + ', ' + ticker + ', ' + open + ', ' + high + ', ' + low + \
                 ', ' + close + ', ' + adj_close + ', ' + volume
        return select

    @classmethod
    def get_where_string(cls, price_date, ticker, start, end, symbol):
        start_str = start.strftime('%Y-%m-%d')
        end_str = end.strftime('%Y-%m-%d')
        where = ''
        where += price_date + ' >= "' + start_str + '" AND ' \
                 + price_date + ' <= "' + end_str + '" AND ('
        where += ticker + ' = "' + symbol + '"' + ' OR '
        where = where[:-4]
        where += ')'
        return where