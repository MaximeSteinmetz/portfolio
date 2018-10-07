
from abc import ABC, abstractmethod


class WeightCriterion(ABC):

    @abstractmethod
    def get_weight(self, mean, std, rfr):
        pass


class HalfKelly(WeightCriterion):

    def __repr__(self):
        return 'Half kelly'

    def get_weight(self, mean, std, rfr):
        return (mean - rfr) / std / std