
def get_mcal_exchange(exchange_str):
    """
    :param exchange_str: Exchange name as in SQL database
    :return: Exchange name as in pandas_market_calendars
    """
    corresp = {'EUREX': ['EPA', 'BSE', 'SWB', 'FWB', 'MIL', 'MAD', 'BCN', 'ELI'],
               'SIX': ['SIX', 'BX'],
               'NYSE': ['NASDAQ', 'NYSE', 'AMEX'],
               'JPX': ['TSE', 'OSE'],
               'LSE': ['LSE'],
               'Not Implemented': ['ASX', 'SSE', 'MOEX', 'OMXH', 'OSL']}
    result = [key for key, value in corresp.items() if exchange_str in value][0]
    return result
