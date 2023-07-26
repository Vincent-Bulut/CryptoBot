import psycopg2
import configparser
import Constants as cte
import logging
import pandas as pd
from collections import namedtuple


class DB_Manager:
    """This class will be linked to postgresql processes"""

    def __init__(self):
        config = configparser.ConfigParser()
        config.read(cte.CONFIG_FILE)
        config.sections()
        self.database = config['DATABASE']['database']
        self.user = config['DATABASE']['user']
        self.password = config['DATABASE']['password']
        self.conn = None
        self.cur = None

    def connect_to_db(self) -> None:
        logging.info('Try to connect to Postgre DB')
        self.conn = psycopg2.connect(database=self.database, user=self.user, password=self.password)

    def create_cursor(self) -> None:
        self.cur = self.conn.cursor()

    def execute_query(self, sql: str) -> bool:
        try:
            self.cur.execute(sql)
            logging.info('Executed Query: ' + sql)
            return True
        except:
            logging.warning('Error Query: ' + sql)
            return False

    def get_query_into_df(self, sql: str) -> pd.DataFrame:
        try:
            self.connect_to_db()
            logging.info('Executed Query: ' + sql)
            df = pd.read_sql_query(sql, self.conn)
            df = df.set_index('date')
            self.conn.close()
            logging.info('conn closed')
            return df
        except:
            logging.warning('Error Query: ' + sql)
            return pd.DataFrame()

    def get_query(self, sql: str) -> list:
        try:
            self.connect_to_db()
            self.create_cursor()
            self.cur.execute(sql)
            logging.info('Executed Query: ' + sql)
            projection = self.cur.fetchall()
            self.cur.close()
            logging.info('cur closed')
            self.conn.close()
            logging.info('conn closed')
            return projection
        except:
            logging.warning('Error Query: ' + sql)
            return []

    def commit_and_close(self) -> None:
        self.conn.commit()
        logging.info('commit done')
        # close the cursor and connection
        self.cur.close()
        logging.info('cur closed')
        self.conn.close()
        logging.info('conn closed')

    def update_streaming_prices(self, symbol_prices: dict) -> None:
        self.connect_to_db()
        self.create_cursor()

        for pairs in symbol_prices:
            symbol = list(pairs.values())[0]
            price = list(pairs.values())[1]

            query = """
            UPDATE ref.staging_prices
            SET price = {}, time = date_trunc('second', now()::timestamp)
            WHERE time = (
            SELECT MAX(time) 
            FROM ref.staging_prices
            WHERE symbol = '{}'
            )
            AND symbol = '{}'
            """.format(price, symbol, symbol)
            try:
                self.execute_query(query)
                logging.info('Executed query: ' + query)
            except:
                logging.warning('Error query :' + query)

        self.commit_and_close()

    def insert_streaming_prices(self, symbol_prices: dict) -> None:
        self.connect_to_db()
        self.create_cursor()

        for pairs in symbol_prices:
            symbol = list(pairs.values())[0]
            price = list(pairs.values())[1]

            query = """
            INSERT INTO ref.staging_prices(symbol, price, time)
            VALUES('{}',{},date_trunc('second', now()::timestamp))
            """.format(symbol, price)
            try:
                print(query)
                self.execute_query(query)
                logging.info('Executed query: ' + query)
            except:
                logging.warning('Error query :' + query)

        self.commit_and_close()

    def insert_histo_prices(self, klines: pd.DataFrame) -> None:
        Prices = namedtuple('prices',
                            ['symbol', 'period', 'open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time',
                             'quote_asset_volume', 'nb_of_shares', 'taker_buy_base_asset_volume',
                             'taker_buy_quote_asset_volume'])
        prices_col = Prices(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)

        self.connect_to_db()
        self.create_cursor()

        for i in range(len(klines)):
            query = """
            INSERT INTO ref.prices(symbol, period, open_time, open, high, low, close, volume, close_time, 
            quote_asset_volume, nb_of_shares, taker_buy_base_asset_volume, taker_buy_quote_asset_volume)
            VALUES('{}','{}','{}',{},{},{},{},{},'{}',{},{},{},{})
            """.format(klines.iloc[i, prices_col.symbol],
                       klines.iloc[i, prices_col.period],
                       klines.iloc[i, prices_col.open_time],
                       klines.iloc[i, prices_col.open],
                       klines.iloc[i, prices_col.high],
                       klines.iloc[i, prices_col.low],
                       klines.iloc[i, prices_col.close],
                       klines.iloc[i, prices_col.volume],
                       klines.iloc[i, prices_col.close_time],
                       klines.iloc[i, prices_col.quote_asset_volume],
                       klines.iloc[i, prices_col.nb_of_shares],
                       klines.iloc[i, prices_col.taker_buy_base_asset_volume],
                       klines.iloc[i, prices_col.taker_buy_quote_asset_volume]
                       )
            try:
                self.execute_query(query)
                logging.info('Executed query: ' + query)
            except:
                logging.warning('Error query :' + query)

        self.commit_and_close()
