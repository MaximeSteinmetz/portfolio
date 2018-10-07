
from abc import ABC, abstractmethod


class Broker(ABC):

    @abstractmethod
    def get_fees(self, quantity, price):
        pass


class BourseDirect(Broker):
    def get_fees(self, quantity, price):
        value = abs(quantity * price)
        if value <= 500:
            return 0.99
        if value <= 1000:
            return 1.90
        if value <= 2000:
            return 2.90
        if value <= 4400:
            return 3.90
        if value > 4400:
            return value * 0.09 / 100


class Binance(Broker):
    def get_fees(self, quantity, price):
        value = abs(quantity * price)
        return value * 0.001


class InterativeBrokers(Broker):

    def get_fees(self, quantity, price):
        pass