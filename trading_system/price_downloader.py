import requests
import datetime as dt
import pytz
from abc import ABC, abstractmethod
from sql import SQL, date_col_sql_data
import time


class DataVendor(ABC):
    """
    Data vendor abstract base class

    Data vendor objects must be able to:
    - download price data
    """

    @abstractmethod
    def download_data(self, freq, instr, symbols, outputsize, start):
        pass


class Poloniex(DataVendor):
    def __init__(self):
        super().__init__()
        self.name = 'Poloniex'
        self.api_key = '3G0YIML88V8F97HA'
        self.timezone = pytz.timezone('UTC')
        self.url = "https://poloniex.com/public"

    def get_info(self, symbol):
        """
        Provides additional info to be inserted in the price data tuple
        """
        dv_id = SQL.get_data_vendor_id(self.name)
        symbol_id = SQL.get_symbol_id(symbol)
        now = dt.datetime.now()
        return dv_id, symbol_id, now

    @classmethod
    def download_data(cls, freq, instr, symbols, outputsize=None, start=None):
        po = Poloniex()
        for symbol in symbols:
            try:
                param = cls.get_api_query(freq, symbol, start)
                print('Downloading ' + symbol + ' data')
                json = po.download_json(param, symbol)
                print(symbol + ' data downloaded')
                data = po.generate_tuple(json, symbol, instr, freq)
                SQL.insert_price_data(data, freq)
                print(symbol + ' data inserted in database')
            except NameError:
                print('Error while querying ' + symbol)
                time.sleep(1)
        SQL.remove_price_duplicates(freq)
        print('Duplicates removed')
        print('Done')

    def download_json(self, param, symbol):
        key = 'Meta Data'
        page = requests.get(self.url, param)
        try:
            bool = len(page.json()) > 0
        except KeyError:
            raise NameError('Error while querying ' + symbol)
        return page.json()

    def generate_tuple(self, json, symbol, instr, freq):
        dv_id, symbol_id, now = self.get_info(symbol)
        data_tuple = []
        for i in json:
            date = dt.datetime.utcfromtimestamp(i['date']).strftime('%Y-%m-%d %H:%M:%S')
            open = i['open']
            high = i['high']
            low = i['low']
            close = i['close']
            volume = i['volume']
            data_tuple.append((symbol_id, symbol, date, open, high, low, close, volume, dv_id, instr, now))
        return data_tuple

    @classmethod
    def get_api_query(cls, freq, symbol, start):
        unixtime = time.mktime(start.timetuple())
        param = {"command": 'returnChartData',
                 "currencyPair": symbol,
                 "end": '9999999999',
                 "period": cls.freq_minutes(freq),
                 "start": unixtime}
        return param

    @staticmethod
    def freq_minutes(freq):
        if freq in ['1min', '5min', '15min', '30min', '60min']:
            return int(freq[:1]) * 60
        elif freq == '1d':
            return 86400


class AlphaVantage(DataVendor):
    def __init__(self):
        super().__init__()
        self.name = 'Alpha Vantage'
        self.api_key = '3G0YIML88V8F97HA'
        self.timezone = pytz.timezone('US/Eastern')
        self.url = "https://www.alphavantage.co/query"

    @classmethod
    def download_data(cls, freq, instr, symbols=None, country=None, outputsize=None, start=None):
        av = cls.get_av_instance(freq, instr)
        if country is not None:
            raw_data = SQL.get_symbols_per_country(country)
            symbols = [item[0] for item in raw_data]
        for symbol in symbols:
            try:
                param = av.get_api_query(freq, symbol, outputsize)
                print('Downloading ' + symbol + ' data')
                json = av.download_json(param, symbol)
                print(symbol + ' data downloaded')
                data = av.generate_tuple(json, symbol, instr, freq)
                data = av.tz_to_utc(data)
                SQL.insert_price_data(data, freq)
                print(symbol + ' data inserted in database')
            except NameError:
                print('Symbol ' + symbol + ' does not exist in Alpha Vantage database')
                time.sleep(1)
        SQL.remove_price_duplicates(freq)
        print('Duplicates removed')
        print('Done')


    @staticmethod
    def get_av_instance(freq, instr):
        """
        :return: AlphaVantage child instance of the particular API call type
        """
        if freq in ['1min', '5min', '15min', '30min', '60min'] and instr == 'stock':
            av = AlphaVantageTimeSeriesIntraday()
        elif freq in ['1d', '7d'] and instr == 'stock':
            av = AlphaVantageTimeSeriesDailyAdjusted()
        elif instr == 'currency':
            av = AlphaVantageCurrencyExchangeRate()
        elif instr == 'digital_currency':
            av = AlphaVantageDigitalCurrencyIntraday()
        return av

    @abstractmethod
    def get_api_query(self, freq, symbol, outputsize):
        if outputsize is None:
            outputsize = 'full'
        return outputsize

    @abstractmethod
    def download_json(self, param, symbol):
        pass

    @abstractmethod
    def generate_tuple(self, json, symbol, instr, freq):
        """
        Converts the json object into a tuple which can be read by MySQL
        :return: tuple contaning price data, from which the columns are identical to those of the
        corresponding SQL table
        """
        pass

    def get_json_empty_checked(self, key, param, symbol):
        """
        Checks for wrong API calls
        :return: json object containing price data
        """
        page = requests.get(self.url, param)
        try:
            bool = page.json()[key] is not None
        except KeyError:
            raise NameError('Symbol ' + symbol + " doesn't exist in Alpha Vantage database.")
        return page.json()

    def get_info(self, symbol):
        """
        Provides additional info to be inserted in the price data tuple
        """
        data_tuple = []
        dv_id = SQL.get_data_vendor_id(self.name)
        symbol_id = SQL.get_symbol_id(symbol)
        now = dt.datetime.now()
        return data_tuple, dv_id, symbol_id, now

    @abstractmethod
    def tz_to_utc(self, data_tuple):
        pass


class AlphaVantageTimeSeriesIntraday(AlphaVantage):

    def get_api_query(self, freq, symbol, outputsize):
        outputsize = super().get_api_query(freq, symbol, outputsize)
        param = {"function": 'TIME_SERIES_INTRADAY',
                 "interval": freq,
                 "symbol": symbol,
                 "outputsize": outputsize,
                 "apikey": self.api_key}
        return param

    def download_json(self, param, symbol):
        key = 'Meta Data'
        return super().get_json_empty_checked(key, param, symbol)

    def generate_tuple(self, json, symbol, instr, freq):
        data_tuple, dv_id, symbol_id, now = super().get_info(symbol)

        time_series_key = list(json)[1]
        time_series = list(json.get(time_series_key).items())

        for i in range(len(time_series)):
            date = time_series[i][0]
            open = list(time_series[i][1].values())[0]
            high = list(time_series[i][1].values())[1]
            low = list(time_series[i][1].values())[2]
            close = list(time_series[i][1].values())[3]
            volume = list(time_series[i][1].values())[4]
            data_tuple.append((symbol_id, symbol, date, open, high, low, close, volume, dv_id, instr, now))
        return data_tuple

    def tz_to_utc(self, data_tuple):
        modified_datetimes = [self.timezone.localize(dt.datetime.strptime(item[date_col_sql_data], "%Y-%m-%d %H:%M:%S"))
                                  .astimezone(pytz.utc) for item in list(data_tuple)]
        row_list = list(data_tuple)
        for index, item in enumerate(row_list):
            row = list(item)
            row[date_col_sql_data] = dt.datetime.strftime(modified_datetimes[index], "%Y-%m-%d %H:%M:%S")
            row_list[index] = tuple(row)
        data_tuple = row_list

        return data_tuple


class AlphaVantageTimeSeriesDailyAdjusted(AlphaVantage):

    def get_api_query(self, freq, symbol, outputsize):
        outputsize = super().get_api_query(freq, symbol, outputsize)
        param = {"function": 'TIME_SERIES_DAILY_ADJUSTED',
                 "symbol": symbol,
                 "outputsize": outputsize,
                 "apikey": self.api_key}
        return param

    def download_json(self, param, symbol):
        key = 'Meta Data'
        return super().get_json_empty_checked(key, param, symbol)

    def generate_tuple(self, json, symbol, instr, freq):
        data_tuple, dv_id, symbol_id, now = super().get_info(symbol)

        time_series_key = list(json)[1]
        time_series = list(json.get(time_series_key).items())

        for i in range(len(time_series)):
            date = time_series[i][0]
            open = list(time_series[i][1].values())[0]
            high = list(time_series[i][1].values())[1]
            low = list(time_series[i][1].values())[2]
            close = list(time_series[i][1].values())[3]
            adj_close = list(time_series[i][1].values())[4]
            volume = list(time_series[i][1].values())[5]
            dividend_amount = list(time_series[i][1].values())[6]
            split_coefficient = list(time_series[i][1].values())[7]
            data_tuple.append((symbol_id, symbol, date, open, high, low, close, adj_close, volume, dividend_amount,
                               split_coefficient, dv_id, instr, now))
        return data_tuple

    def tz_to_utc(self, data_tuple):
        return data_tuple


class AlphaVantageCurrencyExchangeRate(AlphaVantage):

    def get_api_query(self, freq, symbol, outputsize):
        param = {"function": 'CURRENCY_EXCHANGE_RATE',
                 "from_currency": symbol[:3],
                 "to_currency": symbol[3:],
                 "apikey": self.api_key}
        return param

    def download_json(self, param, symbol):
        key = 'Realtime Currency Exchange Rate'
        return super().get_json_empty_checked(key, param, symbol)

    def generate_tuple(self, json, symbol, instr, freq):
        data_tuple, dv_id, symbol_id, now = super().get_info(symbol)

        data_key = list(json)[0]
        data = list(json.get(data_key).items())

        exchange_rate = data[4][1]
        date = data[5][1]
        data_tuple.append((symbol_id, symbol, date, None, None, None, exchange_rate, None, dv_id, instr, now))
        return data_tuple


class AlphaVantageDigitalCurrencyIntraday(AlphaVantage):

    def get_api_query(self, freq, symbol, outputsize):
        param = {"function": 'DIGITAL_CURRENCY_INTRADAY',
                 "symbol": symbol[:3],
                 "market": symbol[3:],
                 "apikey": self.api_key}
        return param

    def download_json(self, param, symbol):
        key = 'To be filled'
        return super().get_json_empty_checked(key, param, symbol)

    def generate_tuple(self, json, symbol, instr, freq):
        data_tuple = []
        return data_tuple