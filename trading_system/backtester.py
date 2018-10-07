

class Backtester:

    @staticmethod
    def generate_trades(signals, sizing_algos, price_dfs, dashboard, strategies, i):
        algos = []
        trades = []
        for index, signal in enumerate(signals):
            for algo in sizing_algos:
                size = algo.process_signal(signal, i, index, price_dfs, dashboard)
                algos.append(size)
            size = min(algos)
            if size != 0:  # critical line for SRD management
                price = price_dfs[index]['close'][i]  # all lines with 'close' critical for "buy at next open" option
                fees = strategies[index].broker.get_fees(size, price)
                trade = Trade(size, price, i, fees, strategies[index].symbol, index)
                trades.append(trade)
        return trades