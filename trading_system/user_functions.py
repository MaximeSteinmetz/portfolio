from sql import SQL
from price_downloader import AlphaVantage, Poloniex
from settings import Settings
from backtester_old import Backtester as b
from plotting import CandlestickPlotter as cp
s = Settings()


def import_data(dv_str, freq, instr, symbols=None, country=None, outputsize=None, start=None):
    """
    :param dv_str: string - name of the data vendor
    :param freq: element of ['1min', '5min', '15min', '30min', '60min', '1d', '7d']
    :param instr: element of ['stock', 'currency', 'digital_currency']
    :param symbols: list of strings containing symbol names. Must match data vendor symbol names
    :param outputsize: element of ['compact', 'full']. To be updated when new data vendor is implemented

    Adds the correponding price data into the SQL database
    """
    dv = Utils.get_data_vendor(dv_str)
    if country is not None:
        data = dv.download_data(freq, instr, country=country, outputsize=outputsize, start=start)
    if symbols is not None:
        data = dv.download_data(freq, instr, symbols=symbols, outputsize=outputsize, start=start)

def remove_price_duplicates(freq):
    """
    :param freq: price frequency of the price table to be cleaned
    Remove the duplicates entries of the price table
    """
    SQL.remove_price_duplicates(freq)
    print('Duplicates removed')

def api_symbol_check(country=None, input_symbols=None):
    if country is not None:
        raw_data = SQL.get_symbols_per_country(country)
        symbols = [item[0] for item in raw_data]
        for symbol in symbols:
            if symbol not in ['AC.PA', 'AI.PA', 'AIR.PA', 'AMS:MT', 'ATO.PA', 'CS.PA', 'BNP.PA', 'EN.PA', 'CAP.PA',
                              'CA.PA', 'ACA.PA', 'BN.PA', 'ENGI.PA', 'EI.PA', 'KER.PA', 'OR.PA', 'VTX:LHN', 'LR.PA',
                              'MC.PA', 'ML.PA', 'ORA.PA', 'RI.PA', 'UG.PA', 'PUB.PA', 'RNO.PA', 'SAF.PA', 'SGO.PA',
                              'SAN.PA', 'SU.PA', 'GLE.PA', 'SW.PA', 'SOLB.PA', 'STM.PA', 'FTI.PA', 'FP.PA', 'AMS:UL',
                              'FR.PA', 'VIE.PA', 'DG.PA', 'VIV.PA']:
                import_data('Alpha Vantage', '1min', 'stock', [symbol], 'compact')


class Utils:
    """
    Support methods to the user functions. Cannot be called directly
    """
    @staticmethod
    def get_data_vendor(dv_str):
        """
        :return: data vendor class
        Functionnality to be implemented when new data vendor are available
        """
        if dv_str.lower() == 'alpha vantage':
            return AlphaVantage
        elif dv_str.lower() == 'poloniex':
            return Poloniex
