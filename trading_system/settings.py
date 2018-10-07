import datetime as dt
from builtins import property
from tzlocal import get_localzone
from abc import ABC, abstractmethod

import pytz
from broker import BourseDirect, InterativeBrokers, Broker, Binance
from sizing_algos import AllInLongOnly
from signal_algos import CloseOvernight, InitIndicatorsAtOpen, CloseAtEnd
from weight_criteria import HalfKelly


class Settings:
    def __init__(self):
        """
        Initiate settings with defaults values. To be modified by user before backtest launch
        """
        self.initial_capital = 10000
        self.capital_currency = 'EUR'
        self._start_dt = 'Not defined'
        self._end_dt = 'Not defined'
        self._freq = 'Not defined'
        self._instr = 'Not defined'
        self.timezone = get_localzone()
        self._lb = 'Not defined'
        self.broker = Binance()
        self._signal_algos = [InitIndicatorsAtOpen(), CloseOvernight(), CloseAtEnd()] # lower index means higher priority
        self.sizing_algos = [AllInLongOnly()]
        self.weight_criterion = HalfKelly()
        self.default_signal_algos = []
        self.risk_free_rate = 4 / 100
        self.nan_perc_limit = 10
        self.accepted_nan = []

    def __repr__(self):
        return 'Settings'

    @property
    def start_dt(self):
        return self._start_dt

    @start_dt.setter
    def start_dt(self, val):
        if isinstance(val, dt.datetime):
            self._start_dt = val
        else:
            raise ValueError('start_dt has received a non-datetime object')

    @property
    def end_dt(self):
        return self._end_dt

    @end_dt.setter
    def end_dt(self, val):
        if isinstance(val, dt.datetime):
            self._end_dt = val
        else:
            raise ValueError('end_dt has received a non-datetime object')

    @property
    def freq(self):
        return self._freq

    @freq.setter
    def freq(self, val):
        if val in ['1min', '5min', '15min', '30min', '60min', '1d', '7d']:
            self._freq = val
        else:
            raise ValueError('freq has received a non-valid string')

    @property
    def instr(self):
        return self._instr

    @instr.setter
    def instr(self, val):
        if val in ['stock', 'currency', 'crypto']:
            self._instr = val
        else:
            raise ValueError('instr has received a non-valid string')

    @property
    def lb(self):
        return self._lb

    @lb.setter
    def lb(self, val):
        if isinstance(val, LbType):
            self._lb = val
        else:
            raise ValueError('lb has received a non-LbType object')

    @property
    def broker(self):
        return self._broker

    @broker.setter
    def broker(self, val):
        if isinstance(val, Broker):
            self._broker = val
        else:
            raise ValueError('broker has received a non-Broker object')

    @property
    def signal_algos(self):
        if self.instr == 'crypto':
            return [CloseAtEnd()]
        else:
            return [InitIndicatorsAtOpen(), CloseOvernight(), CloseAtEnd()]  # lower index means higher priority

    @property
    def start_dt_utc(self):
        return self.timezone.localize(self.start_dt).astimezone(pytz.utc)

    @property
    def end_dt_utc(self):
        return self.timezone.localize(self.end_dt).astimezone(pytz.utc)

    @property
    def start_str_utc(self):
        if self.intraday:
            return self.start_dt_utc.strftime('%Y-%m-%d %H:%M:%S')
        elif  not self.intraday:
            return self.start_dt_utc.strftime('%Y-%m-%d')

    @property
    def end_str_utc(self):
        if self.intraday:
            return self.end_dt_utc.strftime('%Y-%m-%d %H:%M:%S')
        elif not self.intraday:
            return self.end_dt_utc.strftime('%Y-%m-%d')

    @property
    def intraday(self):
        if self.freq in ['1min', '5min', '15min', '30min', '60min']:
            return True
        elif self.freq in ['1d', '7d']:
            return False

    @property
    def freq_integer(self):
        if self.freq not in ['1min', '5min', '15min', '30min', '60min']:
            return 'Not intraday'
        elif self.freq in ['1min', '5min', '15min', '30min', '60min']:
            return int(self.freq[:-3])


    def freq_seconds(self):
        if self.freq in ['1min', '5min', '15min', '30min', '60min']:
            return int(self.freq[:1]) * 60
        elif self.freq == '1d':
            return 86400


class LbType(ABC):
    @abstractmethod
    def do_lb(self):
        pass


class OnceAtStart(LbType):
    def __init__(self):
        self.lb_done = False

    def do_lb(self):
        if not self.lb_done:
            self.lb_done = True
            return True
        else:
            return False


class EveryNBars(LbType):
    def __init__(self, N):
        self.N = N
        self.iter = 1

    def do_lb(self):
        if self.iter == self.N:
            self.iter = 1
            return True
        else:
            self.iter += 1
            return False


class EveryNewDay(LbType):
    pass