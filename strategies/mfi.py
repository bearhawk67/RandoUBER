import pandas as pd
import pandas_ta as ta
import numpy as np

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 100)
pd.set_option("display.width", 1000)

# df = pd.read_csv("TWTR.csv")
# multiplier = 150
# period = 60
# ypos = 2.5
# df["mfi"] = ta.sma(close=((df["Close"] - df["Open"]) / (df["High"] - df["Low"]) * multiplier), length=period) - ypos
# df.to_csv("mfi_test.csv")


def backtest(df_original: pd.DataFrame, multiplier: float, period:int, ypos: float):
    df = df_original.copy()
    df["mfi_1"] = (df["close"] - df["open"])
    df["mfi_2"] = (df["high"] - df["low"])
    df["mfi_3"] = df["mfi_1"] / df["mfi_2"]
    df["mfi_3"] = df["mfi_3"].fillna(0)
    df["mfi_4"] = df["mfi_3"] * multiplier
    df["mfi_5"] = ta.sma(close=df["mfi_4"], length=period)
    df["mfi_b"] = df["mfi_5"] - ypos
    df["mfi"] = ta.sma(close=((df["close"] - df["open"]) / (df["high"] - df["low"]) * multiplier), length=period) - ypos
    # df.to_csv("mfi_test.csv")
    df["signal"] = np.where((df["mfi"] > 10), 1, 0)

    df["pnl"] = df["close"].pct_change() * df["signal"].shift(1)

    df["cum_pnl"] = df['pnl'].cumsum()
    df["max_cum_pnl"] = df["cum_pnl"].cummax()
    df["drawdown"] = df["max_cum_pnl"] - df["cum_pnl"]
    df.to_csv('mfi_pnl.csv')

    return df["pnl"].sum(), df["drawdown"].max()
