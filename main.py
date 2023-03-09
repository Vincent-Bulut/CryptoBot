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
#markets = {'BTCUSDT': 'Bitcoin', 'ETHUSDT': 'Ethereum', 'BNBUSDT': 'Binance USD', 'LTCUSDT': 'Litecoin', 'ADAUSDT': 'Cardano', 'XRPUSDT': 'XRP', 'VETUSDT': 'VeChain', 'MATICUSDT': 'Polygon', 'DOGEUSDT': 'Dogecoin', 'SOLUSDT': 'Solana', 'SHIBUSDT': 'Shiba Inu'}
markets= Crypto().__dict__['tickers']

for symbol, name in markets.items():
    logging.debug(f"Get current price for {name} on binance")
    ticker = cryptos.get_ticker_current_price_on_binance(spot_client, symbol)

    logging.debug(f"Get historical prices for {name} on binance")
    historical_prices = cryptos.get_ticker_historical_prices_on_binance(spot_client, symbol, '1h')

    logging.debug(f"Get orderbook for {name}")
    orderbook = cryptos.get_orderbook(spot_client, symbol)

    print(orderbook.head(2))

    logging.debug(f"Get recent trades for {name}")
    recent_trades = cryptos.get_recent_trades(spot_client, symbol)

    logging.debug(f"Display candles for {name} on binance mode")
    cryptos.display_candles_binance(historical_prices)
    
logging.debug('End of process')