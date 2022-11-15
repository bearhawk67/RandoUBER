from typing import *
from bybit import BybitClient
from models import Contract
import logging
from utils import *
import time
from database import Hdf5Client

logger = logging.getLogger()


def collect_all(client: BybitClient, contract: Contract, from_time: int):

    h5_db = Hdf5Client()
    h5_db.create_dataset(contract)

    # data = h5_db.get_data(contract, from_time=0, to_time=int(time.time()))
    # data = resample_timeframe(data, "15m")
    # print(data)
    # return

    # print(h5_db.get_data(contract, from_time=0, to_time=int(time.time())))
    # return

    oldest_ts, most_recent_ts = h5_db.get_first_last_timestamp(contract)
    print(oldest_ts, most_recent_ts)

    # Initial request
    if oldest_ts is None:
        data = client.get_historical_data(contract, "1", from_time)

        if len(data) == 0:
            logger.warning("%s: no initial data found", contract.symbol)
            return
        else:
            logger.info("%s: Collected %s initial data from %s to %s", contract.symbol, len(data),
                        sec_to_dt(data[0][0]), sec_to_dt(data[-1][0]))
        oldest_ts = data[0][0]
        most_recent_ts = data[-1][0]

        h5_db.write_data(contract, data)

    data_to_insert = []

    # Most recent data
    while True:
        data = client.get_historical_data(contract, "1", int(most_recent_ts + 60))

        if data is None:
            time.sleep(4)  # pause in case an error occurs during the request
            continue

        if len(data) < 2:
            break

        data = data[:-1]

        data_to_insert = data_to_insert + data

        if len(data_to_insert) > 10000:
            h5_db.write_data(contract, data_to_insert)
            data_to_insert.clear()

        if data[-1][0] > most_recent_ts:
            most_recent_ts = data[-1][0]

        logger.info("%s: Collected %s recent data from %s to %s", contract.symbol, len(data),
                    sec_to_dt(data[0][0]), sec_to_dt(data[-1][0]))

        time.sleep(1.1)

    h5_db.write_data(contract, data_to_insert)
    data_to_insert.clear()

    # Older data
    while True:
        data = client.get_historical_data(contract, "1", int(oldest_ts - (60*200)))

        if data is None:
            time.sleep(4)  # pause in case an error occurs during the request
            continue

        if (len(data) == 0) or (data[0][0] == oldest_ts):
            logger.info("%s: No more older data found", contract.symbol)
            break

        if data[-1][0] < oldest_ts:
            oldest_ts = data[0][0]

        data_to_insert = data_to_insert + data

        if len(data_to_insert) > 10000:
            h5_db.write_data(contract, data_to_insert)
            data_to_insert.clear()

        logger.info("%s: Collected %s older data from %s to %s", contract.symbol, len(data),
                    sec_to_dt(data[0][0]), sec_to_dt(data[-1][0]))

        h5_db.write_data(contract, data)

        time.sleep(1.1)

    h5_db.write_data(contract, data_to_insert)
    data_to_insert.clear()
