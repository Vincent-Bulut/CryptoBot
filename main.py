import logging
import Constants as cte
from DB_Manager import DB_Manager as db
from Exchange import Connector, Exchange
from Asset import Crypto
import sys

if __name__ == '__main__':
    logging.basicConfig(format=cte.LOG_FORMAT,
                        datefmt=cte.LOG_DATE_FORMAT,
                        filename=cte.LOG_FILE_NAME,
                        level=logging.DEBUG,
                        filemode='w')

    logging.debug('Starting the process')

    logging.debug('Connexion to Binance Client')
    exchange_con = Connector(cte.BINANCE)
    spot_client = exchange_con.create_client()
    logging.info('Connected to Binance client')

    cryptos = Crypto()
    logging.debug('cryptos setup with: ' + str(len(cryptos.tickers.keys())) + ' cryptocurrency pairs')

    DbMngr = db()

    symbol_prices = cryptos.get_all_ticker_current_price_on_binance(spot_client)

    # Todo implement the merge of the price
    if cte.STREAMING_UPDATE:
        pass
        #DbMngr.update_streaming_prices(symbol_prices)
    else:
        DbMngr.insert_streaming_prices(symbol_prices)

    # period = ['1h', '1w', '1M', '1d']
    period = ['1w']

    for interval in period:
        list_klines = cryptos.get_all_tickers_historical_prices_on_binance(spot_client, interval, 12)

    for klines in list_klines:
        DbMngr.insert_histo_prices(klines)

    logging.debug('End of process')


