import logging
import Constants as cte
import configparser
import pandas as pd
from binance.spot import Spot as Client


class Connector:
    """" This class will build a client to connect to the exchange"""

    def __init__(self, exchange: str):
        self.exchange = exchange

    def create_client(self) -> Client:
        if self.exchange == cte.BINANCE:
            logging.info('Trying to connect to Binance API')
            config = configparser.ConfigParser()
            config.read(cte.CONFIG_FILE)
            config.sections()
            api_url = config['BINANCE']['api_url']
            logging.info('Binance api url : ' + api_url)
            return Client(base_url=api_url)
        else:
            logging.info('The exchange : ' + self.exchange + " has no client setup implemented yet")


class Exchange:

    @staticmethod
    def get_tickers_info_on_binance(client: Client, tickers: list) -> pd.DataFrame:
        exchange_info = client.exchange_info(symbols=tickers)
        exchange_info = pd.DataFrame(exchange_info['symbols'])
        exchange_info = exchange_info.set_index('symbol')
        return exchange_info

    @staticmethod
    def put_tickers_info_on_binance_intoXL(client: Client, tickers: list) -> None:
        exchange_info = client.exchange_info(symbols=tickers)
        exchange_info = pd.DataFrame(exchange_info['symbols'])
        exchange_info.to_excel(cte.PATH_TICKERS_INFO_BINANCE)

