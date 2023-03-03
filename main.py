import logging
import Constants as cte
from Exchange import Connector, Exchange
from Asset import Crypto

if __name__ == '__main__':
    logging.basicConfig(format=cte.LOG_FORMAT,
                        datefmt=cte.LOG_DATE_FORMAT,
                        filename=cte.LOG_FILE_NAME,
                        level=logging.DEBUG,
                        filemode='w')

    logging.debug('Starting the process')

    exchange_con = Connector(cte.BINANCE)
    spot_client = exchange_con.create_client()
    logging.info('Connected to client')

    logging.debug('get Instance Crypto')
    cryptos = Crypto()
    logging.debug('cryptos are setup ' + str(len(cryptos.tickers.keys())) + ' digital assets')

    logging.debug('getting crypto info on binance exchange')
    exchange_info = Exchange.get_tickers_info_on_binance(spot_client, list(cryptos.tickers.keys()))

    Exchange.put_tickers_info_on_binance_intoXL(spot_client, list(cryptos.tickers.keys()))
    logging.debug('crypto info retrieved into XL file ' + cte.PATH_TICKERS_INFO_BINANCE)

    logging.debug('Get current btcusdt price on binance')
    btcusdt_spot = cryptos.get_ticker_current_price_on_binance(spot_client, 'BTCUSDT')

    logging.debug('get btcusdt klines')
    btcusdt_historical_prices = cryptos.get_ticker_historical_prices_on_binance(spot_client, 'BTCUSDT', '1h')

    logging.debug('get orderbook')
    btcusdt_orderbook = cryptos.get_orderbook(spot_client, 'BTCUSDT')

    logging.debug('get recent trades')
    btcusdt_recent_trades = cryptos.get_recent_trades(spot_client, 'BTCUSDT')

    logging.debug('display candles binance mode')
    cryptos.display_candles_binance(btcusdt_historical_prices)

    logging.debug('End of process')

