import pandas as pd
import os
import psycopg2
from collections import namedtuple
import re


def getPeriod(fichier: str) -> str:
    try:
        m = re.search('-1[mdw]+[o]?-', fichier)
        return m.group(0).strip('-')
    except:
        return 'NA'

def getTradingPairs(fichier: str) -> str:
    try:
        m = re.search('[A-Z]+-', fichier)
        return m.group(0).strip('-')
    except:
        return 'NA'

if __name__ == '__main__':
    SOURCE_PATH = "C:\\Users\\vince\\OneDrive\\Bureau\\data\\ToUpload\\"
    arbre = os.walk(SOURCE_PATH)
    listFichiers = []

    Prices = namedtuple('prices',
                        ['symbol', 'period', 'open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time',
                         'quote_asset_volume', 'nb_of_shares', 'taker_buy_base_asset_volume',
                         'taker_buy_quote_asset_volume'])
    prices_col = Prices(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)

    for unRep in arbre:
        for unFic in unRep[2]:
            listFichiers.append(unFic)

    # establish a connection to the database
    conn = psycopg2.connect(
        database="cryptobot",
        user="postgres",
        password="testtest"
    )

    # create a cursor object
    cur = conn.cursor()
    j = 1
    for unFic in listFichiers:
        try:
            print("Downloading prices : " + str(round(j/len(listFichiers)*100, 2)) + '%')
            j += 1
            df = pd.read_csv(SOURCE_PATH + unFic, header=None)
            period = getPeriod(unFic)
            symbol = getTradingPairs(unFic)
            df.columns = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume',
                          'nb_of_shares', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']

            df.insert(0, 'symbol', symbol)
            df.insert(1, 'period', period)
            df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
            df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
            df = df.drop(['ignore'], axis=1)

            for i in range(len(df)):
                sql = """
                INSERT INTO ref.prices(symbol, period, open_time, open, high, low, close, volume, close_time, 
                quote_asset_volume, nb_of_shares, taker_buy_base_asset_volume, taker_buy_quote_asset_volume)
                VALUES('{}','{}','{}',{},{},{},{},{},'{}',{},{},{},{})
                """.format(df.iloc[i, prices_col.symbol],
                           df.iloc[i, prices_col.period],
                           df.iloc[i, prices_col.open_time],
                           df.iloc[i, prices_col.open],
                           df.iloc[i, prices_col.high],
                           df.iloc[i, prices_col.low],
                           df.iloc[i, prices_col.close],
                           df.iloc[i, prices_col.volume],
                           df.iloc[i, prices_col.close_time],
                           df.iloc[i, prices_col.quote_asset_volume],
                           df.iloc[i, prices_col.nb_of_shares],
                           df.iloc[i, prices_col.taker_buy_base_asset_volume],
                           df.iloc[i, prices_col.taker_buy_quote_asset_volume]
                           )

                cur.execute(sql)


        except:
            print('error:' + sql)
            continue

    conn.commit()
    print('commit done')
    # close the cursor and connection
    cur.close()
    print('cur closed')
    conn.close()
    print('conn closed')
