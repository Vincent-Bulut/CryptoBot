from binance.spot import Spot as Client
import pandas as pd
import plotly.graph_objects as go
import json
import matplotlib.pyplot as plt


class Crypto:
    """ This class allows to manage crypto data  """

    def __init__(self):
        self.tickers = {
            'BTCUSDT': 'Bitcoin',
            'ETHUSDT': 'Ethereum',
            'BNBUSDT': 'Binance USD',
            'LTCUSDT': 'Litecoin',
            'ADAUSDT': 'Cardano',
            'XRPUSDT': 'XRP',
            'VETUSDT': 'VeChain',
            'MATICUSDT': 'Polygon',
            'DOGEUSDT': 'Dogecoin',
            'SOLUSDT': 'Solana',
            'SHIBUSDT': 'Shiba Inu'
        }

    @staticmethod
    def get_ticker_current_price_on_binance(client: Client, ticker: str) -> dict:
        return client.ticker_price(ticker)

    def get_all_ticker_current_price_on_binance(self, client: Client) -> dict:
        return client.ticker_price(symbols=list(self.tickers.keys()))

    @staticmethod
    def get_ticker_historical_prices_on_binance(client: Client,
                                                ticker: str,
                                                interval: str,
                                                limit: int = 100) \
            -> pd.DataFrame:

        crypto_history = client.klines(ticker, interval, limit=limit)

        columns = ['time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume',
                   'nb_of_shares', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']
        crypto_history_df = pd.DataFrame(crypto_history, columns=columns)
        crypto_history_df['time'] = pd.to_datetime(crypto_history_df['time'], unit='ms')
        return crypto_history_df

    def get_all_tickers_historical_prices_on_binance(self, client: Client, interval: str, limit: int = 100) -> list:
        list_klines = []

        for symbol in list(self.tickers.keys()):
            klines = client.klines(symbol, interval, limit=limit)
            columns = ['time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume',
                       'nb_of_shares', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']
            crypto_history_df = pd.DataFrame(klines, columns=columns)
            crypto_history_df.insert(0, 'symbol', symbol)
            crypto_history_df.insert(1, 'period', interval)
            crypto_history_df['time'] = pd.to_datetime(crypto_history_df['time'], unit='ms')
            crypto_history_df['close_time'] = pd.to_datetime(crypto_history_df['close_time'], unit='ms')
            crypto_history_df = crypto_history_df.drop(['ignore'], axis=1)
            list_klines.append(crypto_history_df)
        return list_klines

    @staticmethod
    def display_candles_binance(klines: pd.DataFrame):
        fig = go.Figure(data=[go.Candlestick(x=klines['time'],
                                             open=klines['open'],
                                             high=klines['high'],
                                             low=klines['low'],
                                             close=klines['close'])])

        fig.show()

    @staticmethod
    def get_orderbook(client: Client, ticker: str, limit: int = 10) -> pd.DataFrame:
        """ bid/ask """
        depth = client.depth(ticker, limit=limit)
        json_object = json.dumps(depth, indent=8)
        print("The json_object is as below:", json_object)
        depth_df = pd.DataFrame(depth)
        return depth_df

    @staticmethod
    def get_recent_trades(client: Client, ticker: str, limit: int = 10) -> pd.DataFrame:
        # isBuyerMaker = True => sell transaction, isBuyerMaker = False => buy transaction
        trades = client.trades(ticker, limit=limit)

        trades_df = pd.DataFrame(trades)
        trades_df['time'] = pd.to_datetime(trades_df['time'], unit='ms')
        return trades_df

    @staticmethod
    def build_qry_perf_2d(p_from: str, p_to: str, symbol):

        if symbol == "ALL":
            query = """select p1.symbol, date(p1.close_time), p1.close, date(p2.close_time), p2.close,(p1.close/p2.close - 1)*100 as perf
                        from ref.prices p1
                        inner join ref.prices p2 on p1.symbol = p2.symbol
                        where 1=1
                        and p1.period = '1d'
                        and date(p1.close_time) = '{}'
                        and p2.period = '1d'
                        and date(p2.close_time) = '{}'
                        """.format(p_to, p_from)
        else:
            query = """select p1.symbol, date(p1.close_time), p1.close, date(p2.close_time), p2.close,(p1.close/p2.close - 1)*100 as perf
                                    from ref.prices p1
                                    inner join ref.prices p2 on p1.symbol = p2.symbol
                                    where 1=1
                                    and p1.period = '1d'
                                    and date(p1.close_time) = '{}'
                                    and p1.symbol = '{}'
                                    and p2.period = '1d'
                                    and date(p2.close_time) = '{}'
                                    """.format(p_to, symbol, p_from)
        return query

    @staticmethod
    def build_qry_ichimoku_template(symbol: str, p_from: str, p_to: str, periodType: str):
        query = """select case period when '1h' then close_time else date(close_time) end as date, symbol, high, close
                    from ref.prices
                    where symbol = '{}'
                    and date(close_time) between '{}' and '{}'
                    and period = '{}'
                    order by close_time desc""".format(symbol, p_from, p_to, periodType)
        return query

    @staticmethod
    def display_Ichimoku(stock: str, data: pd.DataFrame) -> None:
        high9 = data.high.rolling(9).max()
        low9 = data.low.rolling(9).min()
        high26 = data.high.rolling(26).max()
        low26 = data.low.rolling(26).min()
        high52 = data.high.rolling(52).max()
        low52 = data.low.rolling(52).min()

        data['tenkan_sen'] = (high9 + low9) / 2
        data['kijun_sen'] = (high26 + low26) / 2
        data['senkou_A'] = ((data.tenkan_sen + data.kijun_sen) / 2).shift(26)
        data['senkou_B'] = ((high52 + low52) / 2).shift(26)
        data['chikou'] = data.close.shift(-26)

        data = data.iloc[26:]

        plt.plot(data.index, data['tenkan_sen'], lw=0.8, color='r')
        plt.plot(data.index, data['kijun_sen'], lw=0.8, color='b')
        plt.plot(data.index, data['chikou'], lw=0.8, color='c')
        plt.title('Ichimoku:' + str(stock))
        plt.ylabel("Prices")

        komu = data['close'].plot(lw=1.3, color='k')
        komu.fill_between(data.index, data.senkou_A, data.senkou_B, where=data.senkou_A >= data.senkou_B,
                          color='lightgreen')
        komu.fill_between(data.index, data.senkou_A, data.senkou_B, where=data.senkou_A < data.senkou_B,
                          color='lightcoral')
        plt.grid()
        plt.show()

    @staticmethod
    def get_Ichimoku_data(data: pd.DataFrame) -> pd.DataFrame:
        high9 = data.high.rolling(9).max()
        low9 = data.high.rolling(9).min()
        high26 = data.high.rolling(26).max()
        low26 = data.high.rolling(26).min()
        high52 = data.high.rolling(52).max()
        low52 = data.high.rolling(52).min()

        data['tenkan_sen'] = (high9 + low9) / 2
        data['kijun_sen'] = (high26 + low26) / 2
        data['senkou_A'] = ((data.tenkan_sen + data.kijun_sen) / 2).shift(26)
        data['senkou_B'] = ((high52 + low52) / 2).shift(26)
        data['chikou'] = data.close.shift(-26)

        data = data.iloc[26:]
        return data


