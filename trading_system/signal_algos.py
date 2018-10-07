
from abc import ABC, abstractmethod
import pytz

class SignalAlgo(ABC):

    @abstractmethod
    def treat_signal(self, st, s):
        pass


class CloseOvernight(SignalAlgo):

    def __repr__(self):
        return 'CloseOvernight'

    def treat_signal(self, st, s):
        signals = st.signals
        schedule = st.cal.schedule(st.prices.index[0], st.prices.index[-1])
        if s.intraday:
            closes = [item for item in schedule['market_close']]
            for bar in signals.index:
                for close in closes:
                    if bar == close:
                        signals[bar] = -1
        return signals


class InitIndicatorsAtOpen(SignalAlgo):

    def __repr__(self):
        return 'InitIndicatorsAtOpen'

    def treat_signal(self, st, s):
        signals = st.signals.tz_localize('UTC')
        if s.intraday:
            schedule = st.cal.schedule(st.prices.index[0], st.prices.index[-1])
            opens = [item for item in schedule['market_open']]
            for bar in signals.index:
                for open in opens:
                    try:
                        open_index = st.prices.index.get_loc(open)
                        open_lb_index = open_index + st.bia
                        bar_id = st.prices.index.get_loc(bar)
                        if bar_id >= open_index and bar_id <= open_lb_index:
                            signals[bar] = 0
                    except KeyError:
                        pass
        return signals


class CloseAtEnd(SignalAlgo):

    def __repr__(self):
        return 'CloseAtEnd'

    def treat_signal(self, st, s):
        signals = st.signals
        signals[-1] = -1
        return signals
