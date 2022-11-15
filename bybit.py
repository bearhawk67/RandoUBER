import logging

import pybit
from pybit import usdt_perpetual
from configparser import ConfigParser
import typing
import threading
from models import Contract, Candle
import time
import pandas as pd
import numpy as np

logger = logging.getLogger()


class BybitClient:
    def __init__(self):

        self.prices = dict()
        self.candles = dict()
        self.logs = []
        self.subs = []
        # self.subs_2 = [] use for USD inverse perpetual in the future
        self.timeframes = ['1', '3', '5', '15', '30', '60', '120', '240', '360', 'D', 'W', 'M']
        # int is for body_index, typing.Union allows to indicate multiple values
        # self.strategies: typing.Dict[int, typing.Dict[TechnicalStrategy, GuppyStrategy, TestStrategy,
        #                                               TheWorksStrategy, WolfAndBear]] = dict()
        self.api_key: str
        self.api_secret: str
        self._get_key()
        self._start_http()
        self.contracts = self._get_contracts()
        self.usdt_contracts: typing.List[Contract] = []
        self.btc_contracts: typing.List[str] = []
        # self.ws = self._start_ws()

        # Create subscription list for WebSocket (USDT only)
        perpetual = "USDT"
        for a in self.contracts:
            if a is not None and perpetual in str(a):
                self.subs.append("instrument_info.100ms." + str(a))
                self.btc_contracts.append(str(a))
                self.usdt_contracts.append(str(a))

        for b in self.timeframes:
            for c in self.contracts:
                if c is not None and perpetual in str(c):
                    self.subs.append("candle." + b + '.' + str(c))

        # Initiate Websocket using threading
        # t = threading.Thread(target=self._start_ws, daemon=True)
        # t.start()
        logger.info("Bybit Client successfully initialized")

    def _start_http(self):
        self.session = usdt_perpetual.HTTP(
            # endpoint="https://api-testnet.bybit.com",
            endpoint="https://api.bybit.com",
            api_key=self.api_key,
            api_secret=self.api_secret
        )

    logger.info("Bybit REST API connected")

    def _start_ws(self):
        self.ws = usdt_perpetual.WebSocket(
            test=False,
            api_key=self.api_key,
            api_secret=self.api_secret,
        )
        self.ws.kline_stream(callback=self.handle_position, symbol="BTCUSDT", interval="1")
        # while True:
        #     time.sleep(1)
        logger.info("Bybit WebSocket connection opened")

    def _get_key(self):
        parser = ConfigParser()
        parser.read('config.ini')
        self.api_key = parser.get('api key', 'api_key')
        self.api_secret = parser.get('api key', 'api_secret')

    def _add_log(self, msg: str):
        logger.info("%s", msg)
        self.logs.append({"log": msg, "displayed": False})

    def _get_contracts(self) -> typing.Dict[str, Contract]:
        response_object = self.session.query_symbol()
        contracts = dict()
        try:
            for contract_data in response_object['result']:
                contracts[contract_data['name']] = Contract(contract_data)
        except Exception as e:
            logger.error("Connection error while getting contracts: %s", e)
        # print(contracts)
        return contracts

    def handle_position(self, message):
        print(message)

    def get_historical_candles(self, contract: Contract, timeframe: str) -> typing.List[Candle]:
        # Get last 200 candles on given interval - will not work for monthly or 6min
        tf_equiv = {'1': 60, '3': 180, '5': 300, '6': 360, '15': 900, '30': 1800, '60': 3600, '120': 7200, '240': 14400,
                    '360': 21600, 'D': 86400, 'W': 604800}
        if timeframe != "6":
            if timeframe != "M":
                current_time = int(time.time())
                # Round current time to last closed candle
                current_time = current_time // tf_equiv[timeframe] * tf_equiv[timeframe]
                # set from_t to 200 candles back
                from_t = current_time - (199 * tf_equiv[timeframe])
            # Default monthly to January 1, 2010 @ 12AM
            elif timeframe == "M":
                from_t = 1262304000
            raw_candles = self.session.query_kline(symbol=contract.symbol, interval=timeframe, from_time=from_t)
            candles: typing.List[Candle] = []
            try:
                if raw_candles is not None:
                    for c in raw_candles['result']:
                        d = Candle(c)
                        candles.append(d)
                return candles
            except Exception as e:
                logger.error("Error while getting historical candles for %s on %s interval "
                             "from %s time: %s", contract.symbol, timeframe, from_t, e)
        elif timeframe == '6':
            current_time = int(time.time())

            # Round current time to last closed candle
            current_time = current_time // tf_equiv[timeframe] * tf_equiv[timeframe]
            # set from_t to 200 candles back (will be 400 on 3min TF)
            from_t = current_time - (199 * tf_equiv[timeframe])
            from_t_2 = current_time - (99 * tf_equiv[timeframe])
            raw_candles = self.session.query_kline(symbol=contract.symbol, interval='3', from_time=from_t)
            raw_candles_2 = self.session.query_kline(symbol=contract.symbol, interval='3', from_time=from_t_2)
            candles: typing.List[Candle] = []
            # Convert to a list of Candles
            try:
                if raw_candles is not None:
                    for c in raw_candles['result']:
                        d = Candle(c)
                        candles.append(d)
                if raw_candles_2 is not None:
                    for e in raw_candles_2['result']:
                        f = Candle(e)
                        candles.append(f)
            except Exception as e:
                logger.error("Error while getting historical candles for %s on %s interval "
                             "from %s time: %s", contract.symbol, timeframe, from_t, e)
            try:
                variables = vars(candles[0])
                df = pd.DataFrame([[getattr(i, j) for j in variables] for i in candles], columns=variables)
                df.to_csv('beforeResample.csv')
                df['start'] = pd.to_datetime(df['start'], unit='s', origin="unix")
                df.set_index('start', inplace=True)
                # Look up Pandas offset aliases for different options
                # Convert to 6 minute data
                resample_6min = df.resample('6min').agg({'open': 'first', 'close': 'last', 'high': 'max', 'low': 'min',
                                                         'volume': 'sum'})
                resample_6min.reset_index(inplace=True)
                resample_6min['start'] = resample_6min['start'].values.astype(np.int64) // 10 ** 9
                resample_6min.to_csv('resampled.csv')
                new_dict = resample_6min.to_dict('records')
                # pprint.pprint(new_dict)
                candles.clear()
                candles: typing.List[Candle] = []
                for g in new_dict:
                    new_candle = Candle(g)
                    candles.append(new_candle)
                    return candles
            except Exception as e:
                logger.error("Error when resampling 6 min candles")

    def get_historical_data(self, contract: Contract, timeframe: str, from_time: int):
        raw_candles = self.session.query_kline(symbol=contract.symbol, interval=timeframe, from_time=from_time)
        candles = []
        try:
            if raw_candles is not None:
                for c in raw_candles['result']:
                    candles.append((float(c['start_at']), float(c['open']), float(c['high']), float(c['low']),
                                    float(c['close']), float(c['volume'])))
            return candles
        except Exception as e:
            logger.error("Error while getting historical candles for %s on %s interval "
                         "from %s time: %s", contract.symbol, timeframe, from_time, e)
