from binance.spot import Spot as Client
import pandas as pd
import plotly.graph_objects as go


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

    @staticmethod
    def get_ticker_historical_prices_on_binance(client: Client,
                                                ticker: str,
                                                interval: str,
                                                limit: int = 100) -> pd.DataFrame:
        crypto_history = client.klines(ticker, interval, limit=limit)
        columns = ['time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume',
                   'nb_of_shares', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']
        crypto_history_df = pd.DataFrame(crypto_history, columns=columns)
        crypto_history_df['time'] = pd.to_datetime(crypto_history_df['time'], unit='ms')
        return crypto_history_df

    @staticmethod
    def display_candles_binance(klines: pd.DataFrame):
        fig = go.Figure(data=[go.Candlestick(x=klines['time'],
                                             open=klines['open'],
                                             high=klines['high'],
                                             low=klines['low'],
                                             close=klines['close'])])

        fig.show()

    @staticmethod
    def get_orderbook(client: Client, ticker: str, limit: int=10) -> pd.DataFrame:
        """ bid/ask """
        depth = client.depth(ticker, limit=limit)
        depth_df = pd.DataFrame(depth)
        return depth_df

    @staticmethod
    def get_recent_trades(client: Client, ticker: str, limit: int=10) -> pd.DataFrame:
        # isBuyerMaker = True => sell transaction, isBuyerMaker = False => buy transaction
        trades = client.trades(ticker, limit=limit)
        trades_df = pd.DataFrame(trades)
        trades_df['time'] = pd.to_datetime(trades_df['time'], unit='ms')
        return trades_df
