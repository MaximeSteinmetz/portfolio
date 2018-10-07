
from abc import ABC, abstractmethod


class SizingAlgo(ABC):

    @abstractmethod
    def process_signal(self, signal_value, signal_pos, strat_index, price_dfs, dashboard):
        pass


class AllInLongOnly(SizingAlgo):

    def __repr__(self):
        return 'AllInLongOnly'

    def process_signal(self, signal, bar, st_index, strats, dashboard):
        prices = strats[st_index].prices
        capital_left = dashboard.vacant[st_index]
        position = dashboard.position[st_index]
        quantity = 0

        if signal == 1 and position == 0:
            price = prices['close'][bar]
            quantity = int(capital_left / price)
        elif signal == -1 and position > 0:
            quantity = -position
        return quantity



class AllInLongAndShort(SizingAlgo):

    def __repr__(self):
        return 'AllInLongAndShort'

    def process_signal(self, signal_value, signal_pos, strat_index, price_dfs, dashboard):
        pass