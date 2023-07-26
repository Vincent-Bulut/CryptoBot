from fastapi import FastAPI, Response, BackgroundTasks
from fastapi import Header
from Asset import Crypto
import io
import matplotlib
matplotlib.use('AGG')
import matplotlib.pyplot as plt
from Exchange import Connector
import Constants as cte
from DB_Manager import DB_Manager as db
import pandas as pd

api = FastAPI(
    title="CryptoBot API",
    description="CryptoBot API powered by FastAPI.",
    version="1.0.1",
    openapi_tags=[
    {
        'name': 'DataViz',
        'description': 'Visualize data, Ichimoku analysis'
    },
    {
        'name': 'Performances',
        'description': 'Compute performances'
    },
    {
        'name': 'Streaming',
        'description': 'Get current prices'
    }
])


@api.get('/klines/', tags=['DataViz'])
def display_klines(symbol: str, period: str, increment: int = 100) -> None:
    """Generate X candles from now"""
    cryptos = Crypto()
    exchange_con = Connector(cte.BINANCE)
    spot_client = exchange_con.create_client()
    btcusdt_historical_prices = cryptos.get_ticker_historical_prices_on_binance(spot_client, symbol, period, increment)
    cryptos.display_candles_binance(btcusdt_historical_prices)


def create_img(stock: str, data: pd.DataFrame):
    plt.rcParams['figure.figsize'] = [7.50, 3.50]
    plt.rcParams['figure.autolayout'] = True
    fig = plt.figure()  # make sure to call this, in order to create a new figure
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
    img_buf = io.BytesIO()
    plt.savefig(img_buf, format='png')
    plt.close(fig)
    return img_buf


@api.get('/Ichimoku/', tags=['DataViz'])
def display_klines(symbol: str, dte_from: str, dte_to: str, period: str, background_tasks: BackgroundTasks) -> Response:
    """Display Ichimoku analysis"""
    cryptos = Crypto()
    DbMngr = db()
    qry = cryptos.build_qry_ichimoku_template(symbol, dte_from, dte_to, period)
    df = DbMngr.get_query_into_df(qry)
    data = cryptos.get_Ichimoku_data(df)
    img_buf = create_img(symbol, data)
    background_tasks.add_task(img_buf.close)
    headers = {'Content-Disposition': 'inline; filename="out.png"'}
    return Response(img_buf.getvalue(), headers=headers, media_type='image/png')

@api.get('/Perf/', tags=['Performances'])
def get_perf(dte_from: str, dte_to: str, symbol: str = "ALL") -> list:
    """Compute the performance between two dates for given crypto"""
    cryptos = Crypto()
    DbMngr = db()
    qry = cryptos.build_qry_perf_2d(dte_from, dte_to, symbol)
    lst = DbMngr.get_query(qry)
    return lst

@api.get('/Price/', tags=['Streaming'])
def get_current_prices():
    """Retrieve the last prices for given crypto"""
    exchange_con = Connector(cte.BINANCE)
    spot_client = exchange_con.create_client()
    cryptos = Crypto()
    symbol_prices = cryptos.get_all_ticker_current_price_on_binance(spot_client)
    return symbol_prices