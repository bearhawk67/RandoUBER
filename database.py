import h5py
from models import *
from typing import *
import numpy as np
import logging
import pandas as pd
import time

logger = logging.getLogger()


class Hdf5Client:
    def __init__(self):
        self.hf = h5py.File("data/bybit.h5", "a")
        self.hf.flush()

    def create_dataset(self, contract: Contract):
        if contract.symbol not in self.hf.keys():
            self.hf.create_dataset(contract.symbol, (0, 6), maxshape=(None, 6), dtype="float64")
            self.hf.flush()

    def write_data(self, contract: Contract, data: List[Tuple]):
        min_ts, max_ts = self.get_first_last_timestamp(contract)

        if min_ts is None:
            min_ts = float("inf")
            max_ts = 0

        filtered_data = []

        for d in data:
            if d[0] < min_ts:
                filtered_data.append(d)
            elif d[0] > max_ts:
                filtered_data.append(d)

        if len(filtered_data) == 0:
            logger.warning("%s: No data to insert", contract.symbol)
            return

        data_array = np.array(filtered_data)

        self.hf[contract.symbol].resize(self.hf[contract.symbol].shape[0] + data_array.shape[0], axis=0)
        self.hf[contract.symbol][-data_array.shape[0]:] = data_array

        self.hf.flush()

    def get_first_last_timestamp(self, contract: Contract) -> Union[Tuple[None, None], Tuple[float, float]]:
        existing_data = self.hf[contract.symbol][:]

        if len(existing_data) == 0:
            return None, None

        first_ts = min(existing_data, key=lambda x: x[0])[0]
        last_ts = max(existing_data, key=lambda x: x[0])[0]
        return first_ts, last_ts

    def get_data(self, contract: Contract, from_time: int, to_time: int) -> Union[None, pd.DataFrame]:
        start_query = time.time()

        existing_data = self.hf[contract.symbol][:]

        if len(existing_data) == 0:
            return None

        data = sorted(existing_data, key=lambda x: x[0])
        data = np.array(data)

        df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df = df[(df["timestamp"] >= from_time) & (df["timestamp"] <= to_time)]

        df["timestamp"] = pd.to_datetime(df["timestamp"].values.astype(np.int64), unit="s")
        df.set_index("timestamp", drop=True, inplace=True)

        query_time = round((time.time() - start_query), 2)

        # logger.info("Retrieved %s %s candles from database in %s seconds", len(df.index), contract.symbol, query_time)

        return df
