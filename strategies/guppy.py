import datetime
import typing

from models import *
from utils import *

import pandas as pd
import pandas_ta as ta
import numpy as np
import time
import matplotlib.pyplot as plt
import mplfinance as mpf
import os.path


def backtest(df: pd.DataFrame, initial_capital: int, trade_longs: str, trade_shorts: str, sl_long: float,
             sl_short: float, mfi_long: str, mfi_short: str, mfi_period: int, mfi_mult: int, mfi_ypos: float,
             mfi_long_threshold: float, mfi_short_threshold: float,
             ema200_long: str, ema200_short: str,
             guppy_fast_long: str, guppy_fast_short: str, ribbon_check_long: int, ribbon_check_short: int,
             move_sl_type_long: int, move_sl_type_short: int, move_sl_long: float, move_sl_short: float, risk: int,
             leverage: float, tp_long: int, tp_short: int, ltp1: float, ltp1_qty: float, ltp2: float, ltp2_qty: float,
             ltp3: float, stp1: float, stp1_qty: float, stp2: float, stp2_qty: float, stp3: float, mode: str,
             contract: Contract, tf: str, from_time: int, to_time: int,  macd_long: str, macd_short: str,
             macd_fast: int, macd_slow: int, macd_signal: int, rsi_long: str, rsi_short: str, rsi_length: int,
             rsi_overbought: float, rsi_oversold: float,
             wae_long: str, wae_short: str,
             wae_sensitivity: int, wae_fast_length: int, wae_slow_length: int, wae_bb_length: int, wae_bb_mult: float,
             wae_rma_length: int, wae_dz_mult: float, wae_expl_check: str,
             bb_long: str, bb_short: str, bb_length: int, bb_mult: float,
             adx_long: str, adx_short: str, adx_smoothing: int,
             adx_di_length: int, adx_length_long: float, adx_length_short: float,
             ):

    # Guppy Ribbons
    df["fast_1"] = df.ta.ema(length=3)
    df["fast_2"] = df.ta.ema(length=5)
    df["fast_3"] = df.ta.ema(length=8)
    df["fast_4"] = df.ta.ema(length=10)
    df["fast_5"] = df.ta.ema(length=12)
    df["fast_6"] = df.ta.ema(length=15)
    df["slow_1"] = df.ta.ema(length=30)
    df["slow_2"] = df.ta.ema(length=35)
    df["slow_3"] = df.ta.ema(length=40)
    df["slow_4"] = df.ta.ema(length=45)
    df["slow_5"] = df.ta.ema(length=50)
    df["slow_6"] = df.ta.ema(length=60)
    df["200_EMA"] = df.ta.ema(length=200)

    # Ribbon check
    df["slow_long"] = np.where((df["slow_1"] > df["slow_2"]) & (df["slow_2"] > df["slow_3"]) &
                               (df["slow_3"] > df["slow_4"]) & (df["slow_4"] > df["slow_5"]) &
                               (df["slow_5"] > df["slow_6"]) & (df["slow_6"] != np.NaN), True, False)
    df["slow_short"] = np.where((df["slow_1"] < df["slow_2"]) & (df["slow_2"] < df["slow_3"]) &
                                (df["slow_3"] < df["slow_4"]) & (df["slow_4"] < df["slow_5"]) &
                                (df["slow_5"] < df["slow_6"]) & (df["slow_6"] != np.NaN), True, False)

    if guppy_fast_long.upper() == "Y":
        df["fast_long"] = np.where((df["fast_1"] > df["fast_2"]) & (df["fast_2"] > df["fast_3"]) &
                                   (df["fast_3"] > df["fast_4"]) & (df["fast_4"] > df["fast_5"]) &
                                   (df["fast_5"] > df["fast_6"]) & (df["fast_6"] != np.NaN), True, False)
    else:
        df["fast_long"] = True

    if guppy_fast_short.upper() == "Y":
        df["fast_short"] = np.where((df["fast_1"] < df["fast_2"]) & (df["fast_2"] < df["fast_3"]) &
                                    (df["fast_3"] < df["fast_4"]) & (df["fast_4"] < df["fast_5"]) &
                                    (df["fast_5"] < df["fast_6"]) & (df["fast_6"] != np.NaN), True, False)
    else:
        df["fast_short"] = True

    # EMA Checks
    if ema200_long.upper() == "Y":
        df["200ema_long"] = np.where((df["close"] > df["200_EMA"]) & (df["200_EMA"] != np.NaN), True, False)
    else:
        df["200ema_long"] = True
    if ema200_short.upper() == "Y":
        df["200ema_short"] = np.where((df["close"] < df["200_EMA"]) & (df["200_EMA"] != np.NaN), True, False)
    else:
        df["200ema_short"] = True

    if ribbon_check_long == 1:
        df["band_check_long"] = np.where((df["close"] > df["fast_1"]) & (df["fast_1"] != np.NaN), True, False)
    elif ribbon_check_long == 2:
        df["band_check_long"] = np.where((df["close"] > df["fast_2"]) & (df["fast_2"] != np.NaN), True, False)
    elif ribbon_check_long == 3:
        df["band_check_long"] = np.where((df["close"] > df["fast_3"]) & (df["fast_3"] != np.NaN), True, False)
    elif ribbon_check_long == 4:
        df["band_check_long"] = np.where((df["close"] > df["fast_4"]) & (df["fast_4"] != np.NaN), True, False)
    elif ribbon_check_long == 5:
        df["band_check_long"] = np.where((df["close"] > df["fast_5"]) & (df["fast_5"] != np.NaN), True, False)
    elif ribbon_check_long == 6:
        df["band_check_long"] = np.where((df["close"] > df["fast_6"]) & (df["fast_6"] != np.NaN), True, False)
    elif ribbon_check_long == 7:
        df["band_check_long"] = np.where((df["close"] > df["slow_1"]) & (df["slow_1"] != np.NaN), True, False)
    elif ribbon_check_long == 8:
        df["band_check_long"] = np.where((df["close"] > df["slow_2"]) & (df["slow_2"] != np.NaN), True, False)
    elif ribbon_check_long == 9:
        df["band_check_long"] = np.where((df["close"] > df["slow_3"]) & (df["slow_3"] != np.NaN), True, False)
    elif ribbon_check_long == 10:
        df["band_check_long"] = np.where((df["close"] > df["slow_4"]) & (df["slow_4"] != np.NaN), True, False)
    elif ribbon_check_long == 11:
        df["band_check_long"] = np.where((df["close"] > df["slow_5"]) & (df["slow_5"] != np.NaN), True, False)
    elif ribbon_check_long == 12:
        df["band_check_long"] = np.where((df["close"] > df["slow_6"]) & (df["slow_6"] != np.NaN), True, False)
    else:
        df["band_check_long"] = True

    if ribbon_check_short == 1:
        df["band_check_short"] = np.where((df["close"] < df["fast_1"]) & (df["fast_1"] != np.NaN), True, False)
    elif ribbon_check_short == 2:
        df["band_check_short"] = np.where((df["close"] < df["fast_2"]) & (df["fast_2"] != np.NaN), True, False)
    elif ribbon_check_short == 3:
        df["band_check_short"] = np.where((df["close"] < df["fast_3"]) & (df["fast_3"] != np.NaN), True, False)
    elif ribbon_check_short == 4:
        df["band_check_short"] = np.where((df["close"] < df["fast_4"]) & (df["fast_4"] != np.NaN), True, False)
    elif ribbon_check_short == 5:
        df["band_check_short"] = np.where((df["close"] < df["fast_5"]) & (df["fast_5"] != np.NaN), True, False)
    elif ribbon_check_short == 6:
        df["band_check_short"] = np.where((df["close"] < df["fast_6"]) & (df["fast_6"] != np.NaN), True, False)
    elif ribbon_check_short == 7:
        df["band_check_short"] = np.where((df["close"] < df["slow_1"]) & (df["slow_1"] != np.NaN), True, False)
    elif ribbon_check_short == 8:
        df["band_check_short"] = np.where((df["close"] < df["slow_2"]) & (df["slow_2"] != np.NaN), True, False)
    elif ribbon_check_short == 9:
        df["band_check_short"] = np.where((df["close"] < df["slow_3"]) & (df["slow_3"] != np.NaN), True, False)
    elif ribbon_check_short == 10:
        df["band_check_short"] = np.where((df["close"] < df["slow_4"]) & (df["slow_4"] != np.NaN), True, False)
    elif ribbon_check_short == 11:
        df["band_check_short"] = np.where((df["close"] < df["slow_5"]) & (df["slow_5"] != np.NaN), True, False)
    elif ribbon_check_short == 12:
        df["band_check_short"] = np.where((df["close"] < df["slow_6"]) & (df["slow_6"] != np.NaN), True, False)
    else:
        df["band_check_short"] = True

    # ADX
    adx = df.ta.adx(length=adx_di_length, lensig=adx_smoothing)
    adx.drop("DMP_" + str(adx_di_length), axis=1, inplace=True)
    adx.drop("DMN_" + str(adx_di_length), axis=1, inplace=True)
    df = pd.concat([df, adx], axis=1)

    # ADX entry conditions
    if adx_long.upper() == "Y":
        df["ADX_long"] = np.where((df["ADX_" + str(adx_smoothing)] > adx_length_long), True, False)
    elif adx_long.upper() == "N":
        df["ADX_long"] = True
    if adx_short.upper() == "Y":
        df["ADX_short"] = np.where((df["ADX_" + str(adx_smoothing)] > adx_length_short), True, False)
    elif adx_short.upper() == "N":
        df["ADX_short"] = True

    # Vu Man Chu MFI
    df["mfi_1"] = (df["close"] - df["open"])
    df["mfi_2"] = (df["high"] - df["low"])
    df["mfi_3"] = df["mfi_1"] / df["mfi_2"]
    df["mfi_3"] = df["mfi_3"].fillna(0)
    df["mfi_4"] = df["mfi_3"] * mfi_mult
    df["mfi_5"] = ta.sma(close=df["mfi_4"], length=mfi_period)
    df["MFI"] = df["mfi_5"] - mfi_ypos
    df.drop(["mfi_1", "mfi_2", "mfi_3", "mfi_4", "mfi_5"], axis=1, inplace=True)

    # MFI entry conditions
    if mfi_long.upper() == "Y":
        df["MFI_long"] = np.where((df["MFI"] > mfi_long_threshold), True, False)
    elif mfi_long.upper() == "N":
        df["MFI_long"] = True
    if mfi_short.upper() == "Y":
        df["MFI_short"] = np.where(df["MFI"] < mfi_short_threshold, True, False)
    elif mfi_short.upper() == "N":
        df["MFI_short"] = True

    # Bollinger Bands
    bb = df.ta.bbands(length=bb_length, std=bb_mult)
    bb.drop(["BBB_" + str(bb_length) + "_" + str(bb_mult), "BBP_" + str(bb_length) + "_" + str(float(bb_mult))],
            axis=1, inplace=True)
    df = pd.concat([df, bb], axis=1)
    if bb_long.upper() == "Y":
        df['bb_long'] = np.where((df['BBM_' + str(bb_length) + '_' + str(bb_mult)] >
                                 df['BBM_' + str(bb_length) + '_' + str(bb_mult)].shift(1)) &
                                 (df['low'] < df['BBL_' + str(bb_length) + '_' + str(bb_mult)]), True, False)
    else:
        df['bb_long'] = True
    if bb_short.upper() == "Y":
        df['bb_short'] = np.where((df['BBM_' + str(bb_length) + '_' + str(bb_mult)] <
                                  df['BBM_' + str(bb_length) + '_' + str(bb_mult)].shift(1)) &
                                  (df['high'] > df['BBU_' + str(bb_length) + '_' + str(bb_mult)]), True, False)
    else:
        df['bb_short'] = True

    # MACD
    macd = df.ta.macd(fast=macd_fast, slow=macd_slow, signal=macd_signal, as_mode=True)
    df = pd.concat([df, macd], axis=1)
    if macd_long.upper() == "Y":
        df['MACD_long'] = np.where((df['MACD_'+str(macd_fast)+"_"+str(macd_slow)+"_"+str(macd_signal)] > 0) &
                                   (df['MACDh_'+str(macd_fast)+"_"+str(macd_slow)+"_"+str(macd_signal)] > 0) &
                                   (df['MACDh_'+str(macd_fast)+"_"+str(macd_slow)+"_"+str(macd_signal)] >
                                    df['MACDh_'+str(macd_fast)+"_"+str(macd_slow)+"_"+str(macd_signal)].shift(1)),
                                   True, False)
    else:
        df['MACD_long'] = True
    if macd_short.upper() == "Y":
        df['MACD_short'] = np.where((df['MACD_'+str(macd_fast)+"_"+str(macd_slow)+"_"+str(macd_signal)] < 0) &
                                    (df['MACDh_'+str(macd_fast)+"_"+str(macd_slow)+"_"+str(macd_signal)] < 0) &
                                    (df['MACDh_'+str(macd_fast)+"_"+str(macd_slow)+"_"+str(macd_signal)] <
                                     df['MACDh_'+str(macd_fast)+"_"+str(macd_slow)+"_"+str(macd_signal)].shift(1)),
                                    True, False)
    else:
        df['MACD_short'] = True

    # RSI
    rsi = df.ta.rsi(length=rsi_length)
    df = pd.concat([df, rsi], axis=1)
    if rsi_long.upper() == "Y":
        df['RSI_long'] = np.where((df['RSI_' + str(rsi_length)] < rsi_overbought), True, False)
    else:
        df['RSI_long'] = True
    if rsi_short.upper() == "Y":
        df['RSI_short'] = np.where((df['RSI_' + str(rsi_length)] > rsi_oversold), True, False)
    else:
        df['RSI_short'] = True

    # WAE
    df["tr"] = df.ta.true_range()
    df["deadzone"] = ta.rma(close=df["tr"], length=wae_rma_length) * wae_dz_mult
    macd_1 = df.ta.macd(fast=wae_fast_length, slow=wae_slow_length)
    macd_1.rename(columns={"MACD_" + str(wae_fast_length) + "_" + str(wae_slow_length) + "_9": "macd_1"}, inplace=True)
    macd_1.drop(["MACDh_" + str(wae_fast_length) + "_" + str(wae_slow_length) + "_9", "MACDs_" + str(wae_fast_length)
                 + "_" + str(wae_slow_length) + "_9"], axis=1, inplace=True)
    df = pd.concat([df, macd_1], axis=1)
    df["macd_2"] = df["macd_1"].shift(1)
    df["t1"] = (df["macd_1"] - df["macd_2"]) * wae_sensitivity
    bb = df.ta.bbands(length=wae_bb_length, std=wae_bb_mult)
    bb.drop(["BBM_" + str(wae_bb_length) + "_" + str(float(wae_bb_mult)), "BBB_" + str(wae_bb_length) + "_" +
             str(wae_bb_mult), "BBP_" + str(wae_bb_length) + "_" + str(float(wae_bb_mult))], axis=1, inplace=True)
    df = pd.concat([df, bb], axis=1)
    df["e1"] = (bb["BBU_" + str(wae_bb_length) + "_" + str(float(wae_bb_mult))] -
                bb["BBL_" + str(wae_bb_length) + "_" + str(float(wae_bb_mult))])

    # WAE entry conditions
    if wae_long.upper() == "Y":
        if wae_expl_check.upper() == "Y":
            df["WAE_long"] = np.where((df["t1"] > df["e1"]) & (df["e1"] > df["deadzone"]), True, False)
        else:
            df["WAE_long"] = np.where((df["t1"] > df["e1"]) & (df["t1"] > df["deadzone"]), True, False)
    else:
        df["WAE_long"] = True
    if wae_short.upper() == "Y":
        if wae_expl_check.upper() == "Y":
            df["WAE_short"] = np.where(((-1 * df["t1"]) > df["e1"]) & (df["e1"] > df["deadzone"]), True, False)
        else:
            df["WAE_short"] = np.where(((-1 * df["t1"]) > df["e1"]) & ((-1 * df["t1"]) > df["deadzone"]), True, False)
    else:
        df["WAE_short"] = True

    # Trade signal
    if trade_longs.upper() == "Y" and trade_shorts.upper() == "Y":
        df["trade_signal"] = np.where((df["slow_long"] & df["fast_long"] & df["200ema_long"] & df["band_check_long"]
                                       & df["MFI_long"] & df["MACD_long"] & df["RSI_long"] & df["ADX_long"] &
                                       df["bb_long"] & df["WAE_long"]), "Long",
                                      np.where((df["slow_short"] & df["fast_short"] & df["200ema_short"] &
                                                df["band_check_short"] & df["MFI_short"] & df["MACD_short"] &
                                                df["RSI_short"] & df["ADX_short"] & df["bb_short"] &
                                                df["WAE_short"]), "Short", "No trade"))
    elif trade_longs.upper() == "Y" and trade_shorts.upper() == "N":
        df["trade_signal"] = np.where((df["slow_long"] & df["fast_long"] & df["200ema_long"] & df["band_check_long"] &
                                       df["MFI_long"] & df["MACD_long"] & df["RSI_long"] & df["ADX_long"] &
                                       df["bb_long"] & df["WAE_long"]), "Long", "No trade")
    elif trade_longs.upper() == "N" and trade_shorts.upper() == "Y":
        df["trade_signal"] = np.where((df["slow_short"] & df["fast_short"] & df["200ema_short"] &
                                       df["band_check_short"] & df["MFI_short"] & df["MACD_short"]
                                       & df["RSI_short"] & df["ADX_short"] & df["bb_short"] & df["WAE_short"]),
                                      "Short", "No trade")
    else:
        df["trade_signal"] = "No trade"

    # Calculate PNL
    trade_side = 0
    balance = initial_capital
    entry_time = np.NaN
    entry_price: float = np.NaN
    balance_history: typing.List[float] = []
    balance_history.append(balance)
    trades_won = 0
    trades_lost = 0
    breakeven_trades = 0
    tp1_counter = 0
    tp2_counter = 0
    tp1_hit = False
    tp2_hit = False
    sl_moved = False
    sl_moved_time = np.NaN
    trade_counter = 0
    quantity: float
    entry_fee: float
    exit_fee: float
    tp1_fee: float
    tp2_fee: float
    market_fee = 0.0006
    limit_fee = 0.0001
    df["trades"] = np.NaN
    df["trade_num"] = np.NaN
    df["sl_tracker"] = np.NaN
    df["balance_tracker"] = np.NaN
    df["fees"] = np.NaN
    balance_pct: float
    lev: float
    trade_balance: float
    stop_loss = np.NaN
    rr_long = 0.0
    rr_short = 0.0
    win_loss_tracker: typing.List = ["Start"]
    max_wins = 0
    max_losses = 0
    win_counter = 0
    lose_counter = 0
    max_dd_marker: str
    profit_tracker = [0]
    profit = 0.0
    loss = 0.0
    profit_factor = 0.0
    trade_times: typing.List = []

    # Risk:Reward
    if trade_longs.upper() == "Y":
        if tp_long == 1:
            bp: int
            if 0 < sl_long < 0.2:
                bp = 100
            elif 0.2 <= sl_long < 0.25:
                bp = 80
            elif 0.25 <= sl_long < 0.33:
                bp = 60
            elif 0.33 <= sl_long < 0.4:
                bp = 50
            elif 0.4 <= sl_long < 0.5:
                bp = 50
            elif 0.5 <= sl_long < 0.67:
                bp = 30
            elif 0.67 <= sl_long < 0.8:
                bp = 25
            elif 0.8 <= sl_long < 1.0:
                bp = 20
            elif 1.0 <= sl_long < 1.25:
                bp = 20
            elif 1.25 <= sl_long < 1.33:
                bp = 15
            elif 1.33 <= sl_long < 1.67:
                bp = 15
            elif 1.67 <= sl_long < 2.0:
                bp = 10
            elif 2.0 <= sl_long <= 2.5:
                bp = 10
            else:
                bp = 1
            l_entry = bp * market_fee
            l_win_fee = bp * (1 + ltp1/100) * limit_fee
            l_loss_fee = bp * (1 - sl_long/100) * market_fee
            long_reward = bp * (ltp1/100) - l_entry - l_win_fee
            long_risk = bp * (sl_long/100) + l_entry + l_loss_fee
            rr_long = round(long_reward/long_risk, 3)
        elif tp_long == 2:
            rr_long = ((ltp1 * (ltp1_qty / 100)) + (ltp2 * (ltp2_qty / 100))) / sl_long
        else:
            rr_long = ((ltp1 * (ltp1_qty / 100)) + (ltp2 * (ltp2_qty / 100)) + (ltp3 * ((100 - ltp1_qty - ltp2_qty) /
                                                                                100))) / sl_long
    if trade_shorts.upper() == "Y":
        if tp_short == 1:
            bp: int
            if 0 < sl_short < 0.2:
                bp = 100
            elif 0.2 <= sl_short < 0.25:
                bp = 80
            elif 0.25 <= sl_short < 0.33:
                bp = 60
            elif 0.33 <= sl_short < 0.4:
                bp = 50
            elif 0.4 <= sl_short < 0.5:
                bp = 50
            elif 0.5 <= sl_short < 0.67:
                bp = 30
            elif 0.67 <= sl_short < 0.8:
                bp = 25
            elif 0.8 <= sl_short < 1.0:
                bp = 20
            elif 1.0 <= sl_short < 1.25:
                bp = 20
            elif 1.25 <= sl_short < 1.33:
                bp = 15
            elif 1.33 <= sl_short < 1.67:
                bp = 15
            elif 1.67 <= sl_short < 2.0:
                bp = 10
            elif 2.0 <= sl_short < 2.5:
                bp = 10
            else:
                bp = 1
            s_entry = bp * market_fee
            s_win_fee = bp * (1 - stp1 / 100) * limit_fee
            s_loss_fee = bp * (1 + sl_short / 100) * market_fee
            short_reward = bp * (stp1 / 100) - s_entry - s_win_fee
            short_risk = bp * (sl_short / 100) + s_entry + s_loss_fee
            rr_short = round(short_reward / short_risk, 3)
        elif tp_short == 2:
            rr_short = ((stp1 * (stp1_qty / 100)) + (stp2 * (stp2_qty / 100))) / sl_short
        else:
            rr_short = ((stp1 * (stp1_qty / 100)) + (stp2 * (stp2_qty / 100)) + (stp3 * ((100 - stp1_qty - stp2_qty) /
                                                                                         100))) / sl_short

    # if ((trade_longs.upper() == "Y") and (rr_long < 1.5)) or ((trade_shorts.upper() == "Y") and (rr_short < 1.5)):
    #     if mode == "b":
    #         print(f"Risk:Reward too low. Longs: {rr_long}, Shorts: {rr_short}")
    #     return (initial_capital * -1), 100, 0, rr_long, rr_short, trade_counter, 0, max_losses, max_wins

    # Numpy arrays
    highs = np.array(df['high'])
    lows = np.array(df['low'])
    closes = np.array(df['close'])
    times = np.array(df.index)
    signals = np.array(df['trade_signal'])

    for i in range(len(highs)):
        # Move SL after %
        if (move_sl_type_long == 1) and (closes[i] >= entry_price * (1 + move_sl_long / 100)) and not sl_moved and \
                (trade_side == 1) and (lows[i] > entry_price * (1 - sl_long / 100)):
            df.at[times[i], "sl_tracker"] = "SL Moved to break even"
            sl_moved = True
            sl_moved_time = times[i]
        if (move_sl_type_short == 1) and (closes[i] <= entry_price * (1 - move_sl_short / 100)) and not sl_moved \
                and (trade_side == -1) and (highs[i] < entry_price * (1 + sl_short / 100)):
            df.at[times[i], "sl_tracker"] = "SL Moved to break even"
            sl_moved = True
            sl_moved_time = times[i]

        # Check PNL
        if (signals[i] == "Long") and (trade_side == 0):
            entry_price = closes[i]
            entry_time = times[i]
            trade_side = 1
            trade_counter += 1
            df.at[times[i], "trades"] = "Enter Long"
            df.at[times[i], "trade_num"] = trade_counter
        elif (signals[i] == "Short") and (trade_side == 0):
            entry_price = closes[i]
            entry_time = times[i]
            trade_side = -1
            trade_counter += 1
            df.at[times[i], "trades"] = "Enter Short"
            df.at[times[i], "trade_num"] = trade_counter

        # Risk Calculator
        if trade_side == 1:
            stop_loss = sl_long
        elif trade_side == -1:
            stop_loss = sl_short

        if 0 < stop_loss < 0.2:
            lev = 5
            balance_pct = 100
        elif 0.2 <= stop_loss < 0.25:
            lev = 5
            balance_pct = 80
        elif 0.25 <= stop_loss < 0.33:
            lev = 5
            balance_pct = 60
        elif 0.33 <= stop_loss < 0.4:
            lev = 5
            balance_pct = 50
        elif 0.4 <= stop_loss < 0.5:
            lev = 4
            balance_pct = 50
        elif 0.5 <= stop_loss < 0.67:
            lev = 5
            balance_pct = 30
        elif 0.67 <= stop_loss < 0.8:
            lev = 5
            balance_pct = 25
        elif 0.8 <= stop_loss < 1.0:
            lev = 5
            balance_pct = 20
        elif 1.0 <= stop_loss < 1.25:
            lev = 4
            balance_pct = 20
        elif 1.25 <= stop_loss < 1.33:
            lev = 5
            balance_pct = 15
        elif 1.33 <= stop_loss < 1.67:
            lev = 4
            balance_pct = 15
        elif 1.67 <= stop_loss < 2.0:
            lev = 5
            balance_pct = 10
        elif 2.0 <= stop_loss <= 2.5:
            lev = 4
            balance_pct = 10
        else:
            lev = 1
            balance_pct = 1

        if risk == 2:
            lev *= 2
        elif risk == 3:
            lev *= 3
        elif risk == 4:
            lev *= 4
        elif risk == 5:
            lev *= 5
        elif risk == 6:
            lev = leverage
            balance_pct = 100

        trade_balance = balance * (balance_pct / 100) * lev

        if balance < 10:
            if mode == "b":
                print("Balance liquidated")
                break
            elif mode == "o":
                return (initial_capital * -1), 100, 0, 0, 0, 0, 0, 0, 0

        quantity = trade_balance / entry_price

        entry_fee = trade_balance * market_fee

        # 1 TP
        if tp_long == 1 and trade_side == 1:
            if (lows[i] <= entry_price * (1 - sl_long / 100)) and (times[i] > entry_time) and not sl_moved:
                exit_fee = (quantity * entry_price * (1 - sl_long / 100)) * market_fee
                df.at[times[i], "fees"] = f"{entry_fee}, {exit_fee}"
                balance -= ((((sl_long / 100) * trade_balance) + entry_fee) + exit_fee)
                balance_history.append(balance)
                trades_lost += 1
                trade_side = 0
                entry_price = np.NaN
                entry_time = np.NaN
                sl_moved = False
                sl_moved_time = np.NaN
                df.at[times[i], "balance_tracker"] = balance
                df.at[times[i], "trades"] = "Lose Long"
                if win_loss_tracker[-1] != "L":
                    win_counter = 0
                    lose_counter += 1
                    max_losses = max(max_losses, lose_counter)
                elif win_loss_tracker[-1] == "L":
                    lose_counter += 1
                    max_losses = max(max_losses, lose_counter)
                win_loss_tracker.append("L")
                trade_times.append(pd.to_datetime(df.index[i]))
            elif (lows[i] <= entry_price * 1.002) and (times[i] > entry_time) and sl_moved and \
                    (times[i] > sl_moved_time):
                exit_fee = (quantity * (entry_price * 1.002)) * market_fee
                df.at[times[i], "fees"] = f"{entry_fee}, {exit_fee}"
                balance += (((0.002 * trade_balance) - entry_fee) - exit_fee)
                balance_history.append(balance)
                breakeven_trades += 1
                trade_side = 0
                entry_price = np.NaN
                entry_time = np.NaN
                sl_moved = False
                sl_moved_time = np.NaN
                df.at[times[i], "balance_tracker"] = balance
                df.at[times[i], "trades"] = "Long break even"
                if win_loss_tracker[-1] != "B":
                    win_counter = 0
                    lose_counter = 0
                win_loss_tracker.append("B")
                trade_times.append(pd.to_datetime(df.index[i]))
            else:
                if (highs[i] >= entry_price * (1 + ltp1 / 100)) and (times[i] > entry_time) and \
                        (lows[i] > entry_price * (1 - sl_long / 100)):
                    exit_fee = (quantity * (entry_price * (1 + ltp1 / 100))) * limit_fee
                    df.at[times[i], "fees"] = f"{entry_fee}, {exit_fee}"
                    balance += ((((ltp1 / 100) * trade_balance) - entry_fee) - exit_fee)
                    balance_history.append(balance)
                    trades_won += 1
                    trade_side = 0
                    entry_price = np.NaN
                    entry_time = np.NaN
                    sl_moved = False
                    sl_moved_time = np.NaN
                    df.at[times[i], "balance_tracker"] = balance
                    df.at[times[i], "trades"] = "Win Long"
                    if win_loss_tracker[-1] != "W":
                        win_counter += 1
                        lose_counter = 0
                        max_wins = max(max_wins, win_counter)
                    elif win_loss_tracker[-1] == "W":
                        win_counter += 1
                        max_wins = max(max_wins, win_counter)
                    win_loss_tracker.append("W")
                    trade_times.append(pd.to_datetime(df.index[i]))
        elif tp_short == 1 and trade_side == -1:
            if (highs[i] >= entry_price * (1 + sl_short / 100)) and (times[i] > entry_time) and not sl_moved:
                exit_fee = (quantity * (entry_price * (1 + sl_short / 100))) * market_fee
                df.at[times[i], "fees"] = f"{entry_fee}, {exit_fee}"
                balance -= ((((sl_short / 100) * trade_balance) + entry_fee) + exit_fee)
                balance_history.append(balance)
                trades_lost += 1
                trade_side = 0
                entry_price = np.NaN
                entry_time = np.NaN
                sl_moved = False
                sl_moved_time = np.NaN
                df.at[times[i], "balance_tracker"] = balance
                df.at[times[i], "trades"] = "Lose Short"
                if win_loss_tracker[-1] != "L":
                    win_counter = 0
                    lose_counter += 1
                    max_losses = max(max_losses, lose_counter)
                elif win_loss_tracker[-1] == "L":
                    lose_counter += 1
                    max_losses = max(max_losses, lose_counter)
                win_loss_tracker.append("L")
                trade_times.append(pd.to_datetime(df.index[i]))
            elif (highs[i] >= entry_price * 0.998) and (times[i] > entry_time) and sl_moved and \
                    (times[i] > sl_moved_time):
                exit_fee = (quantity * (entry_price * 0.998)) * market_fee
                df.at[times[i], "fees"] = f"{entry_fee}, {exit_fee}"
                balance += (((0.002 * trade_balance) - entry_fee) - exit_fee)
                balance_history.append(balance)
                breakeven_trades += 1
                trade_side = 0
                entry_price = np.NaN
                entry_time = np.NaN
                sl_moved = False
                sl_moved_time = np.NaN
                df.at[times[i], "balance_tracker"] = balance
                df.at[times[i], "trades"] = "Short break even"
                if win_loss_tracker[-1] != "B":
                    win_counter = 0
                    lose_counter = 0
                win_loss_tracker.append("B")
                trade_times.append(pd.to_datetime(df.index[i]))
            else:
                if (lows[i] <= entry_price * (1 - stp1 / 100)) and (times[i] > entry_time) and \
                        (highs[i] < entry_price * (1 + sl_short / 100)):
                    exit_fee = (quantity * (entry_price * (1 - stp1 / 100))) * limit_fee
                    df.at[times[i], "fees"] = f"{entry_fee}, {exit_fee}"
                    balance += ((((stp1 / 100) * trade_balance) - entry_fee) - exit_fee)
                    balance_history.append(balance)
                    trades_won += 1
                    trade_side = 0
                    entry_price = np.NaN
                    entry_time = np.NaN
                    sl_moved = False
                    sl_moved_time = np.NaN
                    df.at[times[i], "balance_tracker"] = balance
                    df.at[times[i], "trades"] = "Win Short"
                    if win_loss_tracker[-1] != "W":
                        win_counter += 1
                        lose_counter = 0
                        max_wins = max(max_wins, win_counter)
                    elif win_loss_tracker[-1] == "W":
                        win_counter += 1
                        max_wins = max(max_wins, win_counter)
                    win_loss_tracker.append("W")
                    trade_times.append(pd.to_datetime(df.index[i]))

        # 2 TPs
        elif tp_long == 2 and trade_side == 1:
            if (lows[i] <= entry_price * (1 - sl_long / 100)) and times[i] > entry_time and not sl_moved:
                exit_fee = (quantity * (entry_price * (1 - sl_long / 100))) * market_fee
                balance -= (sl_long / 100) * trade_balance - entry_fee - exit_fee
                balance_history.append(balance)
                trades_lost += 1
                trade_side = 0
                entry_price = np.NaN
                entry_time = np.NaN
                df.at[times[i], "trades"] = "Lose Long"
                tp1_hit = False
                sl_moved = False
                sl_moved_time = np.NaN
                df.at[times[i], "balance_tracker"] = balance
            elif (lows[i] <= entry_price * 1.002) and times[i] > entry_time and sl_moved and times[i] > sl_moved_time:
                if tp1_hit:
                    tp1_fee = (quantity * (entry_price * (1 + (ltp1 / 100))) * (ltp1_qty / 100)) * limit_fee
                    exit_fee = quantity * (1 - (ltp1_qty / 100)) * entry_price * 1.002 * market_fee
                    balance += (0.002 * trade_balance * (1 - (ltp1_qty / 100))) + \
                               ((ltp1 / 100) * trade_balance * (ltp1_qty / 100)) - entry_fee - tp1_fee - exit_fee
                else:
                    exit_fee = (quantity * (entry_price * 1.002)) * market_fee
                    balance += 0.002 * trade_balance - entry_fee - exit_fee
                balance_history.append(balance)
                trade_side = 0
                entry_price = np.NaN
                entry_time = np.NaN
                df.at[times[i], "trades"] = "Long hit moved SL"
                tp1_hit = False
                sl_moved = False
                sl_moved_time = np.NaN
                df.at[times[i], "balance_tracker"] = balance
            else:
                if (highs[i] >= entry_price * (1 + ltp1 / 100)) and times[i] > entry_time and not tp1_hit:
                    df.at[times[i], "trades"] = "Long TP1 Hit"
                    tp1_hit = True
                    tp1_counter += 1
                    if move_sl_type_long == 2:
                        df.at[times[i], "sl_tracker"] = "SL Moved to break even"
                        sl_moved = True
                        sl_moved_time = times[i]
                if (highs[i] >= entry_price * (1 + ltp2 / 100)) and times[i] > entry_time:
                    tp1_fee = (quantity * (entry_price * (1 + (ltp1 / 100))) * (ltp1_qty / 100)) * limit_fee
                    exit_fee = quantity * (1 - (ltp1_qty / 100)) * entry_price * (1 + ltp2 / 100) * limit_fee
                    balance += ((ltp1 / 100) * trade_balance * (ltp1_qty / 100)) + \
                               ((ltp2 / 100) * ((100 - ltp1_qty) / 100) * trade_balance) - entry_fee - tp1_fee - \
                               exit_fee
                    balance_history.append(balance)
                    tp1_counter -= 1
                    trades_won += 1
                    trade_side = 0
                    entry_price = np.NaN
                    entry_time = np.NaN
                    df.at[times[i], "trades"] = "Win Long"
                    tp1_hit = False
                    sl_moved = False
                    sl_moved_time = np.NaN
                    df.at[times[i], "balance_tracker"] = balance

        elif tp_short == 2 and trade_side == -1:
            if (highs[i] >= entry_price * (1 + sl_short / 100)) and times[i] > entry_time and not sl_moved:
                exit_fee = (quantity * (entry_price * (1 + sl_short / 100))) * market_fee
                balance -= (sl_short / 100) * trade_balance - entry_fee - exit_fee
                balance_history.append(balance)
                trades_lost += 1
                trade_side = 0
                entry_price = np.NaN
                entry_time = np.NaN
                df.at[times[i], "trades"] = "Lose Short"
                tp1_hit = False
                sl_moved = False
                sl_moved_time = np.NaN
                df.at[times[i], "balance_tracker"] = balance
            elif (highs[i] >= entry_price * 0.998) and times[i] > entry_time and sl_moved and times[i] > sl_moved_time:
                if tp1_hit:
                    tp1_fee = (quantity * (entry_price * (1 - (stp1 / 100))) * (stp1_qty / 100)) * limit_fee
                    exit_fee = quantity * (1 - (stp1_qty/100)) * (entry_price * 0.998) * market_fee
                    balance += 0.002 * trade_balance * (1 - (stp1_qty/100)) + \
                               ((stp1 / 100) * trade_balance * (stp1_qty / 100)) - entry_fee - tp1_fee - exit_fee
                else:
                    exit_fee = quantity * (entry_price * 0.998) * market_fee
                    balance += 0.002 * trade_balance - entry_fee - exit_fee
                balance_history.append(balance)
                trade_side = 0
                entry_price = np.NaN
                entry_time = np.NaN
                df.at[times[i], "trades"] = "Short hit moved SL"
                tp1_hit = False
                sl_moved = False
                sl_moved_time = np.NaN
                df.at[times[i], "balance_tracker"] = balance
            else:
                if (lows[i] <= entry_price * (1 - stp1 / 100)) and times[i] > entry_time and not tp1_hit:
                    df.at[times[i], "trades"] = "Short TP1 Hit"
                    tp1_hit = True
                    tp1_counter += 1
                    if move_sl_type_short == 2:
                        df.at[times[i], "sl_tracker"] = "SL Moved to break even"
                        sl_moved = True
                        sl_moved_time = times[i]
                if (lows[i] <= entry_price * (1 - stp2 / 100)) and times[i] > entry_time:
                    tp1_fee = (quantity * (entry_price * (1 - (stp1 / 100))) * (stp1_qty / 100)) * limit_fee
                    exit_fee = quantity * (1 - (stp1_qty/100)) * (entry_price * (1 - stp2 / 100)) * limit_fee
                    balance += ((stp1 / 100) * trade_balance * (stp1_qty / 100)) + \
                               ((stp2 / 100) * ((100 - stp1_qty) / 100) * trade_balance) - entry_fee - tp1_fee - \
                               exit_fee
                    balance_history.append(balance)
                    trades_won += 1
                    tp1_counter -= 1
                    trade_side = 0
                    entry_price = np.NaN
                    entry_time = np.NaN
                    df.at[times[i], "trades"] = "Win Short"
                    tp1_hit = False
                    sl_moved = False
                    sl_moved_time = np.NaN
                    df.at[times[i], "balance_tracker"] = balance

        # 3 TPs
        elif tp_long == 3 and trade_side == 1:
            if (lows[i] <= entry_price * (1 - sl_long / 100)) and times[i] > entry_time and not sl_moved:
                exit_fee = (quantity * (entry_price * (1 - sl_long / 100))) * market_fee
                balance -= (sl_long / 100) * trade_balance - entry_fee - exit_fee
                balance_history.append(balance)
                trades_lost += 1
                trade_side = 0
                entry_price = np.NaN
                entry_time = np.NaN
                df.at[times[i], "trades"] = "Lose Long"
                tp1_hit = False
                tp2_hit = False
                sl_moved = False
                sl_moved_time = np.NaN
                df.at[times[i], "balance_tracker"] = balance
            elif (lows[i] <= entry_price * 1.002) and times[i] > entry_time and sl_moved and times[i] > sl_moved_time:
                if tp2_hit:
                    tp2_fee = quantity * (ltp2_qty/100) * (entry_price * (1 + (ltp2/100))) * limit_fee
                    tp1_fee = (quantity * (entry_price * (1 + (ltp1 / 100))) * (ltp1_qty / 100)) * limit_fee
                    exit_fee = quantity * (1 - ((ltp1_qty + ltp2_qty)/100)) * (entry_price * 1.002) * market_fee
                    balance += (0.002 * trade_balance * (1 - ((ltp1_qty + ltp2_qty)/100))) + \
                               ((ltp1 / 100) * trade_balance * (ltp1_qty / 100)) + \
                               ((ltp2 / 100) * (ltp2_qty / 100) * trade_balance) - entry_fee - tp1_fee - tp2_fee - \
                               exit_fee
                elif tp1_hit and not tp2_hit:
                    tp1_fee = (quantity * (entry_price * (1 + (ltp1 / 100))) * (ltp1_qty / 100)) * limit_fee
                    exit_fee = quantity * (1 - (ltp1_qty / 100)) * (entry_price * 1.002) * market_fee
                    balance += (0.002 * trade_balance) + ((ltp1 / 100) * trade_balance * (ltp1_qty / 100)) - \
                               entry_fee - tp1_fee - exit_fee
                elif not tp1_hit and not tp2_hit:
                    exit_fee = quantity * entry_price * 1.002 * market_fee
                    balance += 0.002 * trade_balance - exit_fee
                balance_history.append(balance)
                trade_side = 0
                entry_price = np.NaN
                entry_time = np.NaN
                df.at[times[i], "trades"] = "Long hit moved SL"
                tp1_hit = False
                tp2_hit = False
                sl_moved = False
                sl_moved_time = np.NaN
                df.at[times[i], "balance_tracker"] = balance
            else:
                if (highs[i] >= entry_price * (1 + ltp1 / 100)) and times[i] > entry_time and not tp1_hit:
                    df.at[times[i], "trades"] = "Long TP1 Hit"
                    tp1_hit = True
                    tp1_counter += 1
                    if move_sl_type_long == 2:
                        df.at[times[i], "sl_tracker"] = "SL Moved to break even"
                        sl_moved = True
                        sl_moved_time = times[i]
                if (highs[i] >= entry_price * (1 + ltp2 / 100)) and times[i] > entry_time and not tp2_hit:
                    df.at[times[i], "trades"] = "Long TP2 Hit"
                    tp2_hit = True
                    tp2_counter += 1
                    tp1_counter -= 1
                    if move_sl_type_long == 3:
                        df.at[times[i], "sl_tracker"] = "SL Moved to break even"
                        sl_moved = True
                        sl_moved_time = times[i]
                if (highs[i] >= entry_price * (1 + ltp3 / 100)) and times[i] > entry_time:
                    tp2_fee = quantity * (ltp2_qty / 100) * (entry_price * (1 + (ltp2 / 100))) * limit_fee
                    tp1_fee = (quantity * (entry_price * (1 + (ltp1 / 100))) * (ltp1_qty / 100)) * limit_fee
                    exit_fee = quantity * (1 - ((ltp1_qty + ltp2_qty)/100)) * (entry_price * (1 + ltp3 / 100)) * \
                               limit_fee
                    balance += ((ltp1 / 100) * trade_balance * (ltp1_qty / 100)) + \
                               ((ltp2 / 100) * (ltp2_qty / 100) * trade_balance) + \
                               ((ltp3 / 100) * (((100 - ltp1_qty) - ltp2_qty) / 100) * trade_balance) - entry_fee - \
                               tp1_fee - tp2_fee - exit_fee
                    balance_history.append(balance)
                    trades_won += 1
                    tp2_counter -= 1
                    trade_side = 0
                    entry_price = np.NaN
                    entry_time = np.NaN
                    df.at[times[i], "trades"] = "Win Long"
                    tp1_hit = False
                    tp2_hit = False
                    sl_moved = False
                    sl_moved_time = np.NaN
                    df.at[times[i], "balance_tracker"] = balance
        elif tp_short == 3 and trade_side == -1:
            if (highs[i] >= entry_price * (1 + sl_short / 100)) and times[i] > entry_time and not sl_moved:
                exit_fee = (quantity * (entry_price * (1 + sl_short / 100))) * market_fee
                balance -= (sl_short / 100) * trade_balance - entry_fee - exit_fee
                balance_history.append(balance)
                trades_lost += 1
                trade_side = 0
                entry_price = np.NaN
                entry_time = np.NaN
                df.at[times[i], "trades"] = "Short SL hit"
                tp1_hit = False
                tp2_hit = False
                df.at[times[i], "balance_tracker"] = balance
            elif (highs[i] >= entry_price * 0.998) and times[i] > entry_time and sl_moved and times[i] > sl_moved_time:
                if tp2_hit:
                    tp2_fee = quantity * (stp2_qty / 100) * (entry_price * (1 - (stp2 / 100))) * limit_fee
                    tp1_fee = (quantity * (entry_price * (1 + (stp1 / 100))) * (stp1_qty / 100)) * limit_fee
                    exit_fee = quantity * (1 - ((ltp1_qty + ltp2_qty)/100)) * (entry_price * 0.998) * market_fee
                    balance += (0.002 * trade_balance) + ((stp1 / 100) * trade_balance * (stp1_qty / 100)) + \
                               ((stp2 / 100) * (stp2_qty / 100) * trade_balance) - entry_fee - tp1_fee - tp2_fee - \
                               exit_fee
                elif tp1_hit and not tp2_hit:
                    tp1_fee = (quantity * (entry_price * (1 + (stp1 / 100))) * (stp1_qty / 100)) * limit_fee
                    exit_fee = quantity * (1 - (ltp1_qty / 100)) * (entry_price * 0.998) * market_fee
                    balance += (0.002 * trade_balance) + ((stp1 / 100) * trade_balance * (stp1_qty / 100)) - \
                               entry_fee - tp1_fee - exit_fee
                elif not tp1_hit and not tp2_hit:
                    exit_fee = quantity * entry_price * 0.998 * market_fee
                    balance += 0.002 * trade_balance - entry_fee - exit_fee
                balance_history.append(balance)
                trade_side = 0
                entry_price = np.NaN
                entry_time = np.NaN
                df.at[times[i], "trades"] = "Short hit moved SL"
                tp1_hit = False
                tp2_hit = False
                sl_moved = False
                sl_moved_time = np.NaN
                df.at[times[i], "balance_tracker"] = balance
            else:
                if (lows[i] <= entry_price * (1 - stp1 / 100)) and times[i] > entry_time and not tp1_hit:
                    df.at[times[i], "trades"] = "Short TP1 Hit"
                    tp1_hit = True
                    tp1_counter += 1
                    if move_sl_type_short == 2:
                        df.at[times[i], "sl_tracker"] = "SL Moved to break even"
                        sl_moved = True
                        sl_moved_time = times[i]
                elif (lows[i] <= entry_price * (1 - stp2 / 100)) and times[i] > entry_time and not tp2_hit:
                    df.at[times[i], "trades"] = "Short TP2 Hit"
                    tp2_hit = True
                    tp2_counter += 1
                    tp1_counter -= 1
                    if move_sl_type_short == 3:
                        df.at[times[i], "sl_tracker"] = "SL Moved to break even"
                        sl_moved = True
                        sl_moved_time = times[i]
                elif (lows[i] <= entry_price * (1 - stp3 / 100)) and times[i] > entry_time:
                    tp2_fee = quantity * (stp2_qty / 100) * (entry_price * (1 - (stp2 / 100))) * limit_fee
                    tp1_fee = (quantity * (entry_price * (1 + (stp1 / 100))) * (stp1_qty / 100)) * limit_fee
                    exit_fee = quantity * (1 - ((ltp1_qty + ltp2_qty)/100)) * (entry_price * (1 - stp3 / 100)) * \
                               limit_fee
                    balance += ((stp1 / 100) * trade_balance * (stp1_qty / 100)) + \
                               ((stp2 / 100) * (stp2_qty / 100) * trade_balance) + \
                               ((stp3 / 100) * (((100 - stp1_qty) - stp2_qty) / 100) * trade_balance) - entry_fee - \
                               tp1_fee - tp2_fee - exit_fee
                    balance_history.append(balance)
                    trades_won += 1
                    tp2_counter -= 1
                    trade_side = 0
                    entry_price = np.NaN
                    entry_time = np.NaN
                    df.at[times[i], "trades"] = "Win Short"
                    tp1_hit = False
                    tp2_hit = False
                    sl_moved = False
                    sl_moved_time = np.NaN
                    df.at[times[i], "balance_tracker"] = balance
        if (signals[i] == "Long") and (trade_side == 0):
            entry_price = closes[i]
            entry_time = times[i]
            trade_side = 1
            trade_counter += 1
            df.at[times[i], "trades"] += " & Enter Long"
            df.at[times[i], "trade_num"] = trade_counter
        elif (signals[i] == "Short") and (trade_side == 0):
            entry_price = closes[i]
            entry_time = times[i]
            trade_side = -1
            trade_counter += 1
            df.at[times[i], "trades"] += " & Enter Short"
            df.at[times[i], "trade_num"] = trade_counter

    max_pnl = 0.0
    max_drawdown = 0.0

    if trade_counter != 0:
        win_rate = round(trades_won / trade_counter * 100, 2)
        mod_win_rate = round((trades_won + breakeven_trades) / trade_counter * 100, 2)
    else:
        win_rate = 0
        mod_win_rate = 0

    i = 0
    while i < len(balance_history):
        # pnl1 += balance_history[i] - balance_history[i - 1]
        max_pnl = max(max_pnl, balance_history[i])
        max_drawdown = max(max_drawdown, (max_pnl - balance_history[i]) / max_pnl)
        # print(pnl1, max_pnl, max_drawdown)
        i += 1

    j = 1
    while j < len(balance_history):
        profit_tracker.append(balance_history[j]-balance_history[j-1])
        j += 1

    for k in profit_tracker:
        if k > 0:
            profit += k
        else:
            loss += k
    if loss != 0:
        profit_factor = round(profit / abs(loss), 3)
    else:
        profit_factor = 9001

    if max_pnl != 0:
        max_drawdown = round((max_drawdown * 100), 3)
        max_pnl = round(max_pnl, 3) - initial_capital

    pnl = round(((balance / initial_capital) - 1) * 100, 2)
    # day_in_seconds = 86400
    # time_tested = datetime.datetime.timestamp(df.index[-1]) - datetime.datetime.timestamp(df.index[0])
    # num_days = time_tested // day_in_seconds
    # Too Contextual for trading timeframe / style - removing
    # if (num_days * 3) >= trade_counter:
    #     if mode == "b":
    #         print(f"Trades taken: {trade_counter}, Min: {num_days * 3}, Max: {num_days * 10}")
    #         print("Too few trades")
    #     elif mode == "o":
    #         return -float("inf"), float("inf"), -float("inf"), -float("inf"), -float("inf"), 0, -float("inf"), \
    #                float("inf"), -float("inf")
    # if (num_days * 10) <= trade_counter:
    #     if mode == "b":
    #         print(f"Trades taken: {trade_counter}, Min: {num_days * 3}, Max: {num_days * 10}")
    #         print("Too many trades")
    #     elif mode == "o":
    #         return -float("inf"), float("inf"), -float("inf"), -float("inf"), -float("inf"), 0, -float("inf"), \
    #                float("inf"), -float("inf")
    # if win_rate < 30:
    #     if mod_win_rate < 50:
    #         if mode == "b":
    #             print(f"Win rate: {win_rate}")
    #             print("Win rate too low")
    #         elif mode == "o":
    #             return -float("inf"), float("inf"), -float("inf"), -float("inf"), -float("inf"), 0, -float("inf"), \
    #                    float("inf"), -float("inf")
    # if max_losses > 5:
    #     if mode == "b":
    #         print(f"Max losses in a row: {max_losses}")
    #         print("Too many consecutive losses")
    #     elif mode == "o":
    #         return -float("inf"), float("inf"), -float("inf"), -float("inf"), -float("inf"), 0, -float("inf"), \
    #                float("inf"), -float("inf")
    # if max_drawdown > 25:
    #     if mode == "b":
    #         print(f"Max Drawdown too high")
    #     elif mode == "o":
    #         return -float("inf"), float("inf"), -float("inf"), -float("inf"), -float("inf"), 0, -float("inf"), \
    #                float("inf"), -float("inf")
    # if (num_days * (50/3)) > pnl:
    #     if mode == "b":
    #         print(f"PNL: {pnl}, Min: {num_days * (50/3)}")
    #         print("PNL too low")
    #     elif mode == "o":
    #         return -float("inf"), float("inf"), -float("inf"), -float("inf"), -float("inf"), 0, -float("inf"), \
    #                float("inf"), -float("inf")

    if mode == "b":
        if tp_long == 1 and tp_short == 1 and trade_counter != 0:
            print(f"Final balance: ${balance}, Max PNL: ${max_pnl}, Max Drawdown: {max_drawdown}% \n"
                  f"Trades Won: {trades_won}, Trades Lost: {trades_lost}, Break Even "
                  f"Trades: {breakeven_trades}, Total Trades: {trade_counter} \n"
                  f"Win Rate: {win_rate}%, Modified Win Rate: {mod_win_rate}%, Profit Factor: {profit_factor}\n"
                  f"Max Consecutive Wins: {max_wins}, Max Consecutive Losses: {max_losses}\n"
                  f"Risk:Reward (Longs): {rr_long}, Risk:Reward (Shorts): {rr_short}")

        # Cannot calculate win rate for multiple TP strategies until define what a "win" is
        else:
            print(f"Final balance: {balance}, Trades Won: {trades_won}, Trades Lost: {trades_lost}, \n Trades closed "
                  f"after hitting TP1: {tp1_counter}, Trades closed after hitting TP2: {tp2_counter}, Total trades: "
                  f"{trade_counter}, \n Max PNL: ${max_pnl}, Max Drawdown: {max_drawdown}%, "
                  f"Profit Factor: {profit_factor}\n Total Trades: {trade_counter}, "
                  f"Max Consecutive Wins: {max_wins}, Max Consecutive Losses: {max_losses}\n"
                  f"Risk:Reward (Longs): {rr_long}, Risk:Reward (Shorts): {rr_short}")

        start = datetime.datetime.fromtimestamp(from_time + (60 * TF_SECONDS[tf]))
        start = start.strftime("%Y-%m-%d-%I%p")
        end = datetime.datetime.fromtimestamp(to_time)
        end = end.strftime("%Y-%m-%d-%I%p")

        if os.path.exists(f"GuppyBacktest_{contract.symbol}_{tf}_{start}_to_{end}.csv"):
            while True:
                try:
                    myfile = open(f"GuppyBacktest_{contract.symbol}_{tf}_{start}_to_{end}.csv", "w+")
                    break
                except IOError:
                    input(f"Cannot write results to csv file. Please close \nGuppyBacktest_{contract.symbol}_{tf}_"
                          f"{start}_to_{end}.csv \nThen press Enter to "
                          f"retry.")
        df_2 = df.copy()
        df = df.tz_localize(tz='UTC')
        df = df.tz_convert(tz='Japan')
        df = df.tz_localize(tz=None)
        df.to_csv(f"GuppyBacktest_{contract.symbol}_{tf}_{start}_to_{end}.csv")
        color_list = []
        for result in win_loss_tracker:
            if result == "L":
                color_list.append('r')
            elif result == "W":
                color_list.append('g')
            elif result == "B":
                color_list.append('b')
        if len(balance_history) != 0 and max_drawdown != 0:
            i = np.argmax((np.maximum.accumulate(balance_history) - balance_history) / balance_history)
            j = np.argmax(balance_history[:i])
            print(f"Max Drawdown Period: Trade {j} to Trade {i}")
            plt.figure(figsize=(1920 / 96, 1080 / 96), dpi=96)
            plt.plot(balance_history)
            plt.plot([i, j], [balance_history[i], balance_history[j]], 'o', color='Red', markersize=10)
            plt.ylabel("Capital")
            plt.savefig(f"{contract.symbol}_{tf}_{start}_to_{end}.png", bbox_inches='tight')
            # adp = mpf.make_addplot(vline=win_times, color='green')
            # mpf.plot(df_2, type='candle', volume=True, style='yahoo', warn_too_much_data=100000000, figratio=(16, 9),
            #          figscale=2, tight_layout=True, vlines=dict(vlines=trade_times,
            #          colors=color_list, linewidths=1), tz_localize=True)
            plt.show()

        else:
            plt.figure(figsize=(1920 / 96, 1080 / 96), dpi=96)
            plt.plot(balance_history)
            plt.ylabel("Capital")
            plt.savefig(f"{contract.symbol}_{tf}_{start}_to_{end}.png", bbox_inches='tight')
            # adp = mpf.make_addplot(df[['fast_1', 'fast_2', 'fast_3', 'fast_4', 'fast_5', 'fast_6']], type='line')
            # mpf.plot(df_2, type='candle', volume=True, style='yahoo', warn_too_much_data=100000000, figratio=(16, 9),
            #          figscale=2, tight_layout=True, vlines=dict(vlines=trade_times,
            #          colors=color_list, linewidths=1), tz_localize=True)
            plt.show()
    elif mode == "m":
        return pnl, max_drawdown, win_rate, rr_long, rr_short, trade_counter, mod_win_rate, max_losses, max_wins, \
               trades_won, trades_lost, breakeven_trades, profit_factor

    return pnl, max_drawdown, win_rate, rr_long, rr_short, trade_counter, mod_win_rate, max_losses, max_wins
