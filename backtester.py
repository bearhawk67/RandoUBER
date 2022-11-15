from ctypes import *
from models import *
import typing
from database import Hdf5Client
from utils import *
from os.path import exists as file_exists
import pandas as pd
import numpy as np
import strategies.obv
import strategies.ichimoku
import strategies.support_resistance
import strategies.mfi
import strategies.guppy
import random
import os.path
import time
import smtplib
from configparser import ConfigParser
import logging.handlers

parser = ConfigParser()
parser.read('config.ini')
email = parser.get('email', 'email address')

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def run(contract: Contract, strategy: str, tf: str, from_time: int, to_time: int, initial_capital: int):
    exchange = "bybit"
    params_des = STRAT_PARAMS[strategy]
    params = dict()
    input_mode = ["manual", "from csv"]
    while True:
        mode = input(f"Parameter input mode ({', '.join(input_mode)}): ").lower()
        if mode in input_mode:
            break

    if mode == "manual":
        for p_code, p in params_des.items():
            while True:
                if p["type"] == Y_N:
                    params[p_code] = str(input(p["name"] + ": "))
                    if params[p_code] in Y_N:
                        break
                else:
                    try:
                        params[p_code] = p["type"](input(p["name"] + ": "))
                        break
                    except ValueError:
                        continue
    else:
        while True:
            file_name = str(input("Input CSV file name to read parameters from (including .csv): "))

            if file_exists(file_name):
                break
            else:
                print(f"ERROR: {file_name} does not exist")
                continue

        csv_data = pd.read_csv(file_name, header=None, names=["parameter", "value"], index_col="parameter")
        for p_code, p in params_des.items():
            if p["type"] == Y_N:
                params[p_code] = str(csv_data.at[str(p_code), "value"])
            else:
                params[p_code] = p["type"](csv_data.at[str(p_code), "value"])

    if strategy == "obv":
        h5_db = Hdf5Client()
        data = h5_db.get_data(contract, from_time, to_time)
        data = resample_timeframe(data, tf)

        pnl, max_drawdown = strategies.obv.backtest(data, ma_period=params["ma_period"])
        return pnl, max_drawdown

    elif strategy == "ichimoku":
        h5_db = Hdf5Client()
        data = h5_db.get_data(contract, from_time, to_time)
        data = resample_timeframe(data, tf)

        pnl, max_drawdown = strategies.ichimoku.backtest(data, tenkan_period=params["tenkan"],
                                                         kijun_period=params["kijun"])
        return pnl, max_drawdown

    elif strategy == "sup_res":
        h5_db = Hdf5Client()
        data = h5_db.get_data(contract, from_time, to_time)
        data = resample_timeframe(data, tf)

        pnl, max_drawdown = strategies.support_resistance.backtest(data, min_points=params["min_points"],
                                                                   min_diff_points=params["min_diff_points"],
                                                                   rounding_nb=params["rounding_nb"],
                                                                   take_profit=params["take_profit"],
                                                                   stop_loss=params["stop_loss"])
        return pnl, max_drawdown

    elif strategy == "mfi":
        h5_db = Hdf5Client()
        data = h5_db.get_data(contract, from_time, to_time)
        data = resample_timeframe(data, tf)
        pnl, max_drawdown = strategies.mfi.backtest(data, period=params['period'], multiplier=params['multiplier'],
                                                    ypos=params['ypos'])
        return pnl, max_drawdown

    elif strategy == "sma":
        # import C++ library
        lib = get_library()

        obj = lib.Sma_new(exchange.encode(), contract.symbol.encode(), tf.encode(), from_time, to_time)
        lib.Sma_execute_backtest(obj, params["slow_ma"], params["fast_ma"])
        pnl = lib.Sma_get_pnl(obj)
        max_drawdown = lib.Sma_get_max_dd(obj)

        return pnl, max_drawdown

    elif strategy == "guppy":
        h5_db = Hdf5Client()
        data = h5_db.get_data(contract, from_time, to_time)
        data = resample_timeframe(data, tf)
        pnl, max_drawdown, win_rate, rr_long, rr_short, num_trades, mod_win_rate, max_losses, max_wins \
            = strategies.guppy.backtest(df=data, initial_capital=initial_capital,
                                        trade_longs=params['trade_longs'],
                                        trade_shorts=params['trade_shorts'], sl_long=params['sl_long'],
                                        sl_short=params['sl_short'], mfi_long=params['mfi_long'],
                                        mfi_short=params['mfi_short'], mfi_period=params['mfi_period'],
                                        mfi_mult=params['mfi_mult'], mfi_ypos=params['mfi_ypos'],
                                        mfi_long_threshold=params['mfi_long_threshold'],
                                        mfi_short_threshold=params['mfi_short_threshold'],
                                        adx_long=params['adx_long'], adx_short=params['adx_short'],
                                        macd_short=params['macd_short'], macd_fast=params['macd_fast'],
                                        macd_slow=params['macd_slow'], macd_signal=params['macd_signal'],
                                        macd_long=params['macd_long'], rsi_long=params['rsi_long'],
                                        rsi_short=params['rsi_short'], rsi_length=params['rsi_length'],
                                        rsi_overbought=params['rsi_overbought'], rsi_oversold=params['rsi_oversold'],
                                        ema200_long=params['ema200_long'],
                                        ema200_short=params['ema200_short'],
                                        guppy_fast_long=params['guppy_fast_long'],
                                        guppy_fast_short=params['guppy_fast_short'],
                                        ribbon_check_long=params['ribbon_check_long'],
                                        ribbon_check_short=params['ribbon_check_short'],
                                        move_sl_type_long=params['move_sl_type_long'],
                                        move_sl_type_short=params['move_sl_type_short'],
                                        move_sl_long=params['move_sl_long'],
                                        move_sl_short=params['move_sl_short'], risk=params['risk'],
                                        leverage=params['leverage'], tp_long=params['tp_long'],
                                        tp_short=params['tp_short'], ltp1=params['ltp1'],
                                        ltp1_qty=params['ltp1_qty'], ltp2=params['ltp2'],
                                        ltp2_qty=params['ltp2_qty'], ltp3=params['ltp3'],
                                        stp1=params['stp1'], stp1_qty=params['stp1_qty'],
                                        stp2=params['stp2'], stp2_qty=params['stp2_qty'],
                                        stp3=params['stp3'], mode="b", contract=contract, tf=tf,
                                        from_time=from_time, to_time=to_time,
                                        bb_long=params['bb_long'], bb_short=params['bb_short'],
                                        bb_length=params['bb_length'],
                                        bb_mult=params['bb_mult'],
                                        wae_long=params['wae_long'], wae_short=params['wae_short'],
                                        wae_sensitivity=params['wae_sensitivity'],
                                        wae_fast_length=params['wae_fast_length'],
                                        wae_slow_length=params['wae_slow_length'],
                                        wae_bb_length=params['wae_bb_length'],
                                        wae_bb_mult=params['wae_bb_mult'],
                                        wae_rma_length=params['wae_rma_length'],
                                        wae_dz_mult=params['wae_dz_mult'],
                                        wae_expl_check=params['wae_expl_check'],
                                        adx_smoothing=params['adx_smoothing'],
                                        adx_di_length=params['adx_di_length'],
                                        adx_length_long=params['adx_length_long'],
                                        adx_length_short=params['adx_length_short'],)

        return pnl, max_drawdown, win_rate, rr_long, rr_short, num_trades, max_losses, max_wins


def random_start_end(contract: Contract, tf: str, time_delta: float, type:str) -> typing.Tuple[int, int]:
    h5_db = Hdf5Client()
    data_start, data_end = h5_db.get_first_last_timestamp(contract)
    if type == "f":
        period_start = random.randint((data_start + (60 * TF_SECONDS[tf])), (data_end - time_delta))
        period_start -= (60 * TF_SECONDS[tf])
        period_end = int(period_start + time_delta)
    elif type == "l":
        period_start = random.randint((data_end - 31536000), (data_end - time_delta))
        period_start -= (60 * TF_SECONDS[tf])
        period_end = int(period_start + time_delta)
    return period_start, period_end


def params_constraints(strategy: str, tf: str, days: int, hours: int, params: typing.Dict) -> typing.Dict:

    if strategy == "obv":
        pass

    elif strategy == "sup_res":
        pass

    elif strategy == "ichimoku":
        params["kijun"] = max(params["kijun"], params["tenkan"])

    # elif self.strategy == "sma":
    #     params["slow_ma"] = max(params["slow_ma"], params["fast_ma"])

    elif strategy == "guppy":
        # Lengths cannot go over total number of candles in test
        num_candles = int((days * 86400) + (hours * 3600)) // TF_SECONDS[tf]
        if num_candles <= 200:
            params["ema200_long"] = "n"
            params["ema200_short"] = "n"
        if params["mfi_period"] >= num_candles:
            params["mfi_period"] = random.randint(2, num_candles/2)
        if params["macd_fast"] >= num_candles:
            params["macd_fast"] = random.randint(2, num_candles/2)
        if params["macd_slow"] >= num_candles:
            params["macd_slow"] = random.randint(params["macd_fast"], num_candles)
        if params["rsi_length"] >= num_candles:
            params["rsi_length"] = random.randint(2, num_candles/2)
        if params["adx_smoothing"] >= num_candles:
            params["adx_smoothing"] = random.randint(2, num_candles/2)
        if params["adx_di_length"] >= num_candles:
            params["adx_di_length"] = random.randint(2, num_candles/2)
        if params["bb_length"] >= num_candles:
            params["bb_length"] = random.randint(2, num_candles/2)
        if params["wae_fast_length"] >= num_candles:
            params["wae_fast_length"] = random.randint(2, num_candles/2)
        if params["wae_slow_length"] >= num_candles:
            params["wae_slow_length"] = random.randint(params["wae_fast_length"], num_candles)
        if params["wae_bb_length"] >= num_candles:
            params["wae_bb_length"] = random.randint(2, num_candles/2)
        if params["wae_rma_length"] >= num_candles:
            params["wae_rma_length"] = random.randint(2, num_candles/2)

        # Don't turn off both Longs and Shorts
        if (params["trade_longs"].upper() == "N") and (params["trade_shorts"].upper() == "N"):
            choice = random.choice(["trade_longs", "trade_shorts"])
            params[choice] = "y"

        # WAE MACD Slow EMA must be larger than fast EMA
        if params["wae_slow_length"] < params["wae_fast_length"]:
            params["wae_slow_length"], params["wae_fast_length"] = params["wae_fast_length"], \
                                                                   params["wae_slow_length"]

        # TP1 < TP2 < TP3
        params["ltp1"] = min(params["ltp1"], params["ltp2"], params["ltp3"])
        params["ltp3"] = max(params["ltp1"], params["ltp2"], params["ltp3"])
        params["stp1"] = min(params["stp1"], params["stp2"], params["stp3"])
        params["stp3"] = max(params["stp1"], params["stp2"], params["stp3"])
        # Total quantity to take out cannot be more than 100%
        # if params["tp_long"] == 2 and (params["ltp1_qty"] == 100):
        #     while params["ltp1_qty"] == 100:
        #         params["ltp1_qty"] = round(random.uniform(params["ltp1_qty"]["min"], params["ltp1_qty"]["max"]),
        #                                    params["ltp1_qty"]["decimals"])
        # if params["tp_short"] == 2 and (params["stp1_qty"] == 100):
        #     while params["stp1_qty"] == 100:
        #         params["stp1_qty"] = round(random.uniform(params["stp1_qty"]["min"], params["stp1_qty"]["max"]),
        #                                    params["stp1_qty"]["decimals"])
        if params["tp_long"] == 3 and ((params["ltp1_qty"] + params["ltp2_qty"]) > 100):
            # while (params["ltp1_qty"] + params["ltp2_qty"]) >= 100:
            #     params["ltp1_qty"] = round(random.uniform(params["ltp1_qty"]["min"], params["ltp1_qty"]["max"]),
            #                                params["ltp1_qty"]["decimals"])
            #     params["ltp2_qty"] = round(random.uniform(params["ltp2_qty"]["min"], params["ltp2_qty"]["max"]),
            #                                params["ltp2_qty"]["decimals"])
            params["ltp2_qty"] = 100 - params["ltp1_qty"]
        if params["tp_short"] == 3 and ((params["stp1"] + params["stp2"]) > 100):
            # while (params["stp1_qty"] + params["stp2_qty"]) >= 100:
            #     params["stp1_qty"] = round(random.uniform(params["stp1_qty"]["min"], params["stp1_qty"]["max"]),
            #                                params["stp1_qty"]["decimals"])
            #     params["stp2_qty"] = round(random.uniform(params["stp2_qty"]["min"], params["stp2_qty"]["max"]),
            #                                params["stp2_qty"]["decimals"])
            params["stp2_qty"] = 100 - params["stp1_qty"]

        # Maintain safe R:R
        # if (params["sl_long"] == 0.1) and (params["ltp1"] < 0.4):
        #     params["ltp1"] = round(random.uniform(0.4, 5.0), 1)
        # elif (params["sl_long"] >= 0.2) and (params["sl_long"] <= 0.5) and \
        #         (params["ltp1"] < (3 * params["sl_long"])):
        #     params["ltp1"] = round(random.uniform(3 * params["sl_long"], 5.0), 1)
        # elif (params["sl_long"] >= 0.6) and (params["sl_long"] <= 2.5) and \
        #         (params["ltp1"] < (2 * params["sl_long"])):
        #     params["ltp1"] = round(random.uniform(2 * params["sl_long"], 5.0), 1)
        # elif params["sl_long"] > 3.1:
        #     params["sl_long"] = round(random.uniform(0.1, 2.5), 1)
        #     params["ltp1"] = round(random.uniform(2 * params["sl_long"], 5.0), 1)
        # if (params["sl_short"] == 0.1) and (params["stp1"] < 0.4):
        #     params["stp1"] = round(random.uniform(0.4, 5.0), 1)
        # elif (params["sl_short"] >= 0.2) and (params["sl_short"] <= 0.5) and \
        #         (params["stp1"] < (3 * params["sl_short"])):
        #     params["stp1"] = round(random.uniform(3 * params["sl_short"], 5.0), 1)
        # elif (params["sl_short"] >= 0.6) and (params["sl_short"] <= 2.5) and \
        #         (params["stp1"] < (2 * params["sl_short"])):
        #     params["stp1"] = round(random.uniform(2 * params["sl_short"], 5.0), 1)
        # elif params["sl_short"] > 2.5:
        #     params["sl_short"] = round(random.uniform(0.1, 2.5), 1)
        #     params["stp1"] = round(random.uniform(2 * params["sl_short"], 5.0), 1)

        rr_long: float
        rr_short: float
        market_fee = 0.0006
        limit_fee = 0.0001
        i = 0
        for i in range(20):
            bpl: int
            bps: int
            if 0 < params["sl_long"] < 0.2:
                bpl = 100
            elif 0.2 <= params["sl_long"] < 0.25:
                bpl = 80
            elif 0.25 <= params["sl_long"] < 0.33:
                bpl = 60
            elif 0.33 <= params["sl_long"] < 0.4:
                bpl = 50
            elif 0.4 <= params["sl_long"] < 0.5:
                bpl = 50
            elif 0.5 <= params["sl_long"] < 0.67:
                bpl = 30
            elif 0.67 <= params["sl_long"] < 0.8:
                bpl = 25
            elif 0.8 <= params["sl_long"] < 1.0:
                bpl = 20
            elif 1.0 <= params["sl_long"] < 1.25:
                bpl = 20
            elif 1.25 <= params["sl_long"] < 1.33:
                bpl = 15
            elif 1.33 <= params["sl_long"] < 1.67:
                bpl = 15
            elif 1.67 <= params["sl_long"] < 2.0:
                bpl = 10
            elif 2.0 <= params["sl_long"] <= 2.5:
                bpl = 10
            else:
                bpl = 1
            l_entry = bpl * market_fee
            l_win_fee = bpl * (1 + params["ltp1"] / 100) * limit_fee
            l_loss_fee = bpl * (1 - params["sl_long"] / 100) * market_fee
            long_reward = bpl * (params["ltp1"] / 100) - l_entry - l_win_fee
            long_risk = bpl * (params["sl_long"] / 100) + l_entry + l_loss_fee
            rr_long = round(long_reward / long_risk, 3)

            if 0 < params["sl_short"] < 0.2:
                bps = 100
            elif 0.2 <= params["sl_short"] < 0.25:
                bps = 80
            elif 0.25 <= params["sl_short"] < 0.33:
                bps = 60
            elif 0.33 <= params["sl_short"] < 0.4:
                bps = 50
            elif 0.4 <= params["sl_short"] < 0.5:
                bps = 50
            elif 0.5 <= params["sl_short"] < 0.67:
                bps = 30
            elif 0.67 <= params["sl_short"] < 0.8:
                bps = 25
            elif 0.8 <= params["sl_short"] < 1.0:
                bps = 20
            elif 1.0 <= params["sl_short"] < 1.25:
                bps = 20
            elif 1.25 <= params["sl_short"] < 1.33:
                bps = 15
            elif 1.33 <= params["sl_short"] < 1.67:
                bps = 15
            elif 1.67 <= params["sl_short"] < 2.0:
                bps = 10
            elif 2.0 <= params["sl_short"] <= 2.5:
                bps = 10
            else:
                bps = 1
            s_entry = bps * market_fee
            s_win_fee = bps * (1 - params["stp1"] / 100) * limit_fee
            s_loss_fee = bps * (1 + params["sl_short"] / 100) * market_fee
            short_reward = bps * (params["stp1"] / 100) - s_entry - s_win_fee
            short_risk = bps * (params["sl_short"] / 100) + s_entry + s_loss_fee
            rr_short = round(short_reward / short_risk, 3)
            if (1.5 < rr_long) and (rr_long < 15) and (1.5 < rr_short) and (rr_short < 15):
                # print("RR ok")
                break
            if (rr_long > 15) or (rr_long < 1.5):
                params["sl_long"] = round(random.uniform(0.1, 2.5), 1)
                params["ltp1"] = round(random.uniform(0.2, 5.0), 1)
                # print(f"{rr_long} Recalculating RR long")
            if (rr_short > 15) or (rr_short < 1.5):
                params["sl_short"] = round(random.uniform(0.1, 2.5), 1)
                params["stp1"] = round(random.uniform(0.2, 5.0), 1)
                # print(f"{rr_short} Recalculating RR short")

        # # MFI multiplier must be large enough that YPOS doesn't skew signals to shorts
        # if params["mfi_mult"] < (params["mfi_ypos"] * 60):
        #     params["mfi_mult"] = params["mfi_ypos"] * 60

        # MACD Fast EMA must be shorter than Slow
        if params["macd_fast"] > params["macd_slow"]:
            params["macd_fast"], params["macd_slow"] = params["macd_slow"], params["macd_fast"]

    return params


def multitest(contract: Contract, strategy: str, tf: str, days: int, hours: int, initial_capital: int,
              num_results: int, pool: str):
    exchange = "bybit"
    params_des = STRAT_PARAMS[strategy]
    params = dict()
    # Minimum PNL to multitest results
    while True:
        min_pnl = input("Minimum acceptable PNL to move on to Multitest or Press Enter to not set a minimum: ")
        try:
            if min_pnl == "":
                min_pnl = -100.0
                break
            else:
                min_pnl = float(min_pnl)
                break
        except ValueError:
            continue

    # Minimum Average PNL to keep results
    while True:
        min_avg_pnl = input("Minimum acceptable Average PNL or Press Enter to not set a minimum: ")
        try:
            if min_avg_pnl == "":
                min_avg_pnl = -100.0
                break
            else:
                min_avg_pnl = float(min_avg_pnl)
                break
        except ValueError:
            continue

    # Minimum % Positive to keep results
    while True:
        min_percent_positive = input("Minimum acceptable % Positive or Press Enter to not set a minimum: ")
        try:
            if min_percent_positive == "":
                min_percent_positive = 0.0
                break
            else:
                min_percent_positive = float(min_percent_positive)
                break
        except ValueError:
            continue

    # Minimum Number of Trades (average) to keep results
    while True:
        min_trades = input("Minimum average number of trades or Press Enter to not set a minimum: ")
        try:
            if min_trades == "":
                min_trades = 0.0
                break
            else:
                min_trades = float(min_trades)
                break
        except ValueError:
            continue

    test_start_time = time.time()

    test_results = pd.DataFrame()
    saved_parameters = pd.DataFrame()

    # Loop until minimum initial PNL requirement met
    pnl = -100
    results = 0

    while results < num_results:
        i = 0
        while pnl <= min_pnl:
            # Generate a set of random parameters
            backtest = BacktestResult()
            for p_code, p in params_des.items():
                if p["type"] == int:
                    backtest.parameters[p_code] = random.randint(p["min"], p["max"])
                elif p["type"] == float:
                    backtest.parameters[p_code] = round(random.uniform(p["min"], p["max"]), p["decimals"])
                elif p["type"] == Y_N:
                    backtest.parameters[p_code] = random.choice(p["choices"])
                    # if result == 0:
                    #     backtest.parameters[p_code] = "N"
                    # else:
                    #     backtest.parameters[p_code] = "Y"

            backtest.parameters = params_constraints(strategy, tf, days, hours, backtest.parameters)
            pnl = initial_tester(contract, strategy, tf, days, hours, initial_capital, backtest.parameters, "m",
                                 pool)
            if i % 4 == 0:
                print(f"\r{results} results complete. Running.", end=" ")
            elif (i % 4 == 1) or (i % 4 == 3):
                print(f"\r{results} results complete. Running..", end=" ")
            elif i % 4 == 2:
                print(f"\r{results} results complete. Running...", end=" ")
            i += 1

        m_results = tester(contract, strategy, tf, days, hours, 50, initial_capital, backtest.parameters,
                           "s", pool, results)
        last_result = m_results.drop(m_results.index[:-1])
        last_result = last_result.reset_index()
        last_result = last_result.iloc[:, 1:]

        pnl = -100
        if (last_result['pnl_avg'][0] > min_avg_pnl) and (last_result['%_positive'][0] > min_percent_positive) and \
                (last_result['num_trades_avg'][0] > min_trades):
            test_results = pd.concat([test_results, last_result], axis=0)
            saved_parameter = pd.Series(backtest.parameters)
            saved_parameter = pd.DataFrame(saved_parameter)
            saved_parameter = saved_parameter.transpose()
            saved_parameters = pd.concat([saved_parameters, saved_parameter], axis=0)
            results += 1
            print("\rMultitest passed", end=" ")
        else:
            print("\rMultitest failed", end=" ")

    if os.path.exists(f"FreedomFinder_Multitest_{contract.symbol}_{tf}_{days}d_{hours}h.csv"):
        while True:
            try:
                myfile = open(f"FreedomFinder_Multitest_{contract.symbol}_{tf}_{days}d_{hours}h.csv", "w+")
                break
            except IOError:
                input(f"Cannot write results to csv file. Please close \n"
                      f"FreedomFinder_Multitest_{contract.symbol}_{tf}_{days}d_{hours}h.csv\nThen press Enter to "
                      f"retry.")
    test_results.to_csv(f"FreedomFinder_Multitest_{contract.symbol}_{tf}_{days}d_{hours}h.csv")
    if os.path.exists(f"FreedomFinder_Parameters_{contract.symbol}_{tf}_{days}d_{hours}h.csv"):
        while True:
            try:
                myfile = open(f"FreedomFinder_Parameters_{contract.symbol}_{tf}_{days}d_{hours}h.csv", "w+")
                break
            except IOError:
                input(f"Cannot write results to csv file. Please close \n"
                      f"FreedomFinder_Parameters_{contract.symbol}_{tf}_{days}d_{hours}h.csv\nThen press Enter to "
                      f"retry.")
    saved_parameters.to_csv(f"FreedomFinder_Parameters_{contract.symbol}_{tf}_{days}d_{hours}h.csv")
    try:
        smtp = smtplib.SMTP("smtp.gmail.com", 587)
        smtp.starttls()
        smtp.login("guppy.bot.messenger@gmail.com", "otqcxemvnbxvfjpe")
        subject = "FreedomFinder run complete"
        text = f"FreedomFinder for {contract.symbol}, {tf}, {days} days, {hours} hours complete."
        message = "Subject: {} \n\n {}".format(subject, text)
        smtp.sendmail("guppy.bot.messenger@gmail.com", email, message)
        smtp.quit()
    except smtplib.SMTPException as e:
        logger.error(e)
        smtp = smtplib.SMTP("smtp.gmail.com", 587)
        smtp.starttls()
        smtp.login("guppy.bot.messenger@gmail.com", "otqcxemvnbxvfjpe")
        subject = "FreedomFinder run complete"
        text = f"FreedomFinder for {contract.symbol}, {tf}, {days} days, {hours} hours complete."
        message = "Subject: {} \n\n {}".format(subject, text)
        smtp.sendmail("guppy.bot.messenger@gmail.com", email, message)
        smtp.quit()


def initial_tester(contract: Contract, strategy: str, tf: str, days: int, hours: int, initial_capital: int,
           params: typing.Dict, mode: str, pool: str) -> float:

    day_seconds = days * 24 * 60 * 60
    hour_seconds = hours * 60 * 60
    total_seconds = day_seconds + hour_seconds

    if pool == "last year":
        db = Hdf5Client()
        oldest_ts, most_recent_ts = db.get_first_last_timestamp(contract)
        from_time, to_time = random_start_end(contract, tf, total_seconds, "l")
    else:
        from_time, to_time = random_start_end(contract, tf, total_seconds, "f")

    if strategy == "guppy":
        h5_db = Hdf5Client()
        data = h5_db.get_data(contract, from_time, to_time)
        data = resample_timeframe(data, tf)
        pnl, max_drawdown, win_rate, rr_long, rr_short, num_trades, mod_win_rate, max_losses, max_wins, \
        trades_won, trades_lost, breakeven_trades, profit_factor \
            = strategies.guppy.backtest(df=data, initial_capital=initial_capital,
                                        trade_longs=params['trade_longs'],
                                        trade_shorts=params['trade_shorts'], sl_long=params['sl_long'],
                                        sl_short=params['sl_short'], mfi_long=params['mfi_long'],
                                        mfi_short=params['mfi_short'], mfi_period=params['mfi_period'],
                                        mfi_mult=params['mfi_mult'], mfi_ypos=params['mfi_ypos'],
                                        mfi_long_threshold=params['mfi_long_threshold'],
                                        mfi_short_threshold=params['mfi_short_threshold'],
                                        macd_short=params['macd_short'], macd_fast=params['macd_fast'],
                                        macd_slow=params['macd_slow'], macd_signal=params['macd_signal'],
                                        macd_long=params['macd_long'], rsi_long=params['rsi_long'],
                                        rsi_short=params['rsi_short'], rsi_length=params['rsi_length'],
                                        rsi_overbought=params['rsi_overbought'], rsi_oversold=params['rsi_oversold'],
                                        ema200_long=params['ema200_long'],
                                        ema200_short=params['ema200_short'],
                                        guppy_fast_long=params['guppy_fast_long'],
                                        guppy_fast_short=params['guppy_fast_short'],
                                        ribbon_check_long=params['ribbon_check_long'],
                                        ribbon_check_short=params['ribbon_check_short'],
                                        move_sl_type_long=params['move_sl_type_long'],
                                        move_sl_type_short=params['move_sl_type_short'],
                                        move_sl_long=params['move_sl_long'],
                                        move_sl_short=params['move_sl_short'], risk=params['risk'],
                                        leverage=params['leverage'], tp_long=params['tp_long'],
                                        tp_short=params['tp_short'], ltp1=params['ltp1'],
                                        ltp1_qty=params['ltp1_qty'], ltp2=params['ltp2'],
                                        ltp2_qty=params['ltp2_qty'], ltp3=params['ltp3'],
                                        stp1=params['stp1'], stp1_qty=params['stp1_qty'],
                                        stp2=params['stp2'], stp2_qty=params['stp2_qty'],
                                        stp3=params['stp3'], mode="m", contract=contract, tf=tf,
                                        from_time=from_time, to_time=to_time,
                                        bb_long=params['bb_long'], bb_short=params['bb_short'],
                                        bb_length=params['bb_length'],
                                        bb_mult=params['bb_mult'],
                                        wae_long=params['wae_long'], wae_short=params['wae_short'],
                                        wae_sensitivity=params['wae_sensitivity'],
                                        wae_fast_length=params['wae_fast_length'],
                                        wae_slow_length=params['wae_slow_length'],
                                        wae_bb_length=params['wae_bb_length'],
                                        wae_bb_mult=params['wae_bb_mult'],
                                        wae_rma_length=params['wae_rma_length'],
                                        wae_dz_mult=params['wae_dz_mult'],
                                        wae_expl_check=params['wae_expl_check'],
                                        adx_long=params['adx_long'], adx_short=params['adx_short'],
                                        adx_smoothing=params['adx_smoothing'],
                                        adx_di_length=params['adx_di_length'],
                                        adx_length_long=params['adx_length_long'],
                                        adx_length_short=params['adx_length_short'],
                                        )
        return pnl


def tester(contract: Contract, strategy: str, tf: str, days: int, hours: int, tests: int, initial_capital: int,
           params: typing.Dict, mode: str, pool: str, num_results: int) -> pd.DataFrame:

    df = pd.DataFrame()
    pnl_list = []
    pnl_avg = []
    pnl_std = []
    pnl_cv = []
    max_dd_list = []
    max_dd_avg = []
    max_dd_std = []
    max_dd_cv = []
    win_rate_list = []
    win_rate_avg = []
    win_rate_std = []
    win_rate_cv = []
    mod_win_rate_list = []
    mod_win_rate_avg = []
    mod_win_rate_std = []
    mod_win_rate_cv = []
    num_trades_list = []
    num_trades_avg = []
    num_trades_std = []
    num_trades_cv = []
    max_losses_list = []
    max_losses_avg = []
    max_losses_std = []
    max_losses_cv = []
    max_wins_list = []
    max_wins_avg = []
    max_wins_std = []
    max_wins_cv = []
    trades_won_list = []
    trades_won_avg = []
    trades_won_std = []
    trades_won_cv = []
    trades_lost_list = []
    trades_lost_avg = []
    trades_lost_std = []
    trades_lost_cv = []
    breakeven_trades_list = []
    breakeven_trades_avg = []
    breakeven_trades_std = []
    breakeven_trades_cv = []
    profit_factor_list = []
    profit_factor_avg = []
    profit_factor_std = []
    profit_factor_cv = []

    from_time_list = []
    to_time_list = []
    positive = 0
    negative = 0
    from_time_timestamps = []
    pnl_percent_pos = []

    day_seconds = days * 24 * 60 * 60
    hour_seconds = hours * 60 * 60
    total_seconds = day_seconds + hour_seconds

    if pool == "last year":
        db = Hdf5Client()
        oldest_ts, most_recent_ts = db.get_first_last_timestamp(contract)
        from_time, to_time = random_start_end(contract, tf, total_seconds, "l")
    else:
        from_time, to_time = random_start_end(contract, tf, total_seconds, "f")

    if strategy == "guppy":
        h5_db = Hdf5Client()
        data = h5_db.get_data(contract, from_time, to_time)
        data = resample_timeframe(data, tf)
        pnl, max_drawdown, win_rate, rr_long, rr_short, num_trades, mod_win_rate, max_losses, max_wins, \
        trades_won, trades_lost, breakeven_trades, profit_factor \
            = strategies.guppy.backtest(df=data, initial_capital=initial_capital,
                                        trade_longs=params['trade_longs'],
                                        trade_shorts=params['trade_shorts'], sl_long=params['sl_long'],
                                        sl_short=params['sl_short'], mfi_long=params['mfi_long'],
                                        mfi_short=params['mfi_short'], mfi_period=params['mfi_period'],
                                        mfi_mult=params['mfi_mult'], mfi_ypos=params['mfi_ypos'],
                                        mfi_long_threshold=params['mfi_long_threshold'],
                                        mfi_short_threshold=params['mfi_short_threshold'],
                                        macd_short=params['macd_short'], macd_fast=params['macd_fast'],
                                        macd_slow=params['macd_slow'], macd_signal=params['macd_signal'],
                                        macd_long=params['macd_long'], rsi_long=params['rsi_long'],
                                        rsi_short=params['rsi_short'], rsi_length=params['rsi_length'],
                                        rsi_overbought=params['rsi_overbought'], rsi_oversold=params['rsi_oversold'],
                                        ema200_long=params['ema200_long'],
                                        ema200_short=params['ema200_short'],
                                        guppy_fast_long=params['guppy_fast_long'],
                                        guppy_fast_short=params['guppy_fast_short'],
                                        ribbon_check_long=params['ribbon_check_long'],
                                        ribbon_check_short=params['ribbon_check_short'],
                                        move_sl_type_long=params['move_sl_type_long'],
                                        move_sl_type_short=params['move_sl_type_short'],
                                        move_sl_long=params['move_sl_long'],
                                        move_sl_short=params['move_sl_short'], risk=params['risk'],
                                        leverage=params['leverage'], tp_long=params['tp_long'],
                                        tp_short=params['tp_short'], ltp1=params['ltp1'],
                                        ltp1_qty=params['ltp1_qty'], ltp2=params['ltp2'],
                                        ltp2_qty=params['ltp2_qty'], ltp3=params['ltp3'],
                                        stp1=params['stp1'], stp1_qty=params['stp1_qty'],
                                        stp2=params['stp2'], stp2_qty=params['stp2_qty'],
                                        stp3=params['stp3'], mode="m", contract=contract, tf=tf,
                                        from_time=from_time, to_time=to_time,
                                        bb_long=params['bb_long'], bb_short=params['bb_short'],
                                        bb_length=params['bb_length'],
                                        bb_mult=params['bb_mult'],
                                        wae_long=params['wae_long'], wae_short=params['wae_short'],
                                        wae_sensitivity=params['wae_sensitivity'],
                                        wae_fast_length=params['wae_fast_length'],
                                        wae_slow_length=params['wae_slow_length'],
                                        wae_bb_length=params['wae_bb_length'],
                                        wae_bb_mult=params['wae_bb_mult'],
                                        wae_rma_length=params['wae_rma_length'],
                                        wae_dz_mult=params['wae_dz_mult'],
                                        wae_expl_check=params['wae_expl_check'],
                                        adx_long=params['adx_long'], adx_short=params['adx_short'],
                                        adx_smoothing=params['adx_smoothing'],
                                        adx_di_length=params['adx_di_length'],
                                        adx_length_long=params['adx_length_long'],
                                        adx_length_short=params['adx_length_short'],
                                        )

    i = 0
    while i < tests:
        if pool == "last year":
            db = Hdf5Client()
            oldest_ts, most_recent_ts = db.get_first_last_timestamp(contract)
            from_time, to_time = random_start_end(contract, tf, total_seconds, "l")
        else:
            from_time, to_time = random_start_end(contract, tf, total_seconds, "f")
        for j in from_time_timestamps:
            while True:
                if abs(from_time - j) < 86400:
                    if pool == "last year":
                        from_time, to_time = random_start_end(contract, tf, total_seconds, "l")
                    else:
                        from_time, to_time = random_start_end(contract, tf, total_seconds, "f")
                else:
                    break
        if strategy == "guppy":
            h5_db = Hdf5Client()
            data = h5_db.get_data(contract, from_time, to_time)
            data = resample_timeframe(data, tf)
            pnl, max_drawdown, win_rate, rr_long, rr_short, num_trades, mod_win_rate, max_losses, max_wins, \
                trades_won, trades_lost, breakeven_trades, profit_factor \
                = strategies.guppy.backtest(df=data, initial_capital=initial_capital,
                                            trade_longs=params['trade_longs'],
                                            trade_shorts=params['trade_shorts'], sl_long=params['sl_long'],
                                            sl_short=params['sl_short'], mfi_long=params['mfi_long'],
                                            mfi_short=params['mfi_short'], mfi_period=params['mfi_period'],
                                            mfi_mult=params['mfi_mult'], mfi_ypos=params['mfi_ypos'],
                                            mfi_long_threshold=params['mfi_long_threshold'],
                                            mfi_short_threshold=params['mfi_short_threshold'],
                                            macd_short=params['macd_short'], macd_fast=params['macd_fast'],
                                            macd_slow=params['macd_slow'], macd_signal=params['macd_signal'],
                                            macd_long=params['macd_long'], rsi_long=params['rsi_long'],
                                            rsi_short=params['rsi_short'], rsi_length=params['rsi_length'],
                                            rsi_overbought=params['rsi_overbought'], rsi_oversold=params['rsi_oversold'],
                                            ema200_long=params['ema200_long'],
                                            ema200_short=params['ema200_short'],
                                            guppy_fast_long=params['guppy_fast_long'],
                                            guppy_fast_short=params['guppy_fast_short'],
                                            ribbon_check_long=params['ribbon_check_long'],
                                            ribbon_check_short=params['ribbon_check_short'],
                                            move_sl_type_long=params['move_sl_type_long'],
                                            move_sl_type_short=params['move_sl_type_short'],
                                            move_sl_long=params['move_sl_long'],
                                            move_sl_short=params['move_sl_short'], risk=params['risk'],
                                            leverage=params['leverage'], tp_long=params['tp_long'],
                                            tp_short=params['tp_short'], ltp1=params['ltp1'],
                                            ltp1_qty=params['ltp1_qty'], ltp2=params['ltp2'],
                                            ltp2_qty=params['ltp2_qty'], ltp3=params['ltp3'],
                                            stp1=params['stp1'], stp1_qty=params['stp1_qty'],
                                            stp2=params['stp2'], stp2_qty=params['stp2_qty'],
                                            stp3=params['stp3'], mode="m", contract=contract, tf=tf,
                                            from_time=from_time, to_time=to_time,
                                            bb_long=params['bb_long'], bb_short=params['bb_short'],
                                            bb_length=params['bb_length'],
                                            bb_mult=params['bb_mult'],
                                            wae_long=params['wae_long'], wae_short=params['wae_short'],
                                            wae_sensitivity=params['wae_sensitivity'],
                                            wae_fast_length=params['wae_fast_length'],
                                            wae_slow_length=params['wae_slow_length'],
                                            wae_bb_length=params['wae_bb_length'],
                                            wae_bb_mult=params['wae_bb_mult'],
                                            wae_rma_length=params['wae_rma_length'],
                                            wae_dz_mult=params['wae_dz_mult'],
                                            wae_expl_check=params['wae_expl_check'],
                                            adx_long=params['adx_long'], adx_short=params['adx_short'],
                                            adx_smoothing=params['adx_smoothing'],
                                            adx_di_length=params['adx_di_length'],
                                            adx_length_long=params['adx_length_long'],
                                            adx_length_short=params['adx_length_short'],
                                            )

            if pnl > 0:
                positive += 1
            else:
                negative += 1
            pnl_percent_pos.append((positive / (i + 1)) * 100)
            pnl_list.append(pnl)
            pnl_avg.append(np.mean(pnl_list))
            pnl_std.append(np.std(pnl_list))
            if np.mean(pnl_list) != 0:
                pnl_cv.append(np.std(pnl_list)/np.mean(pnl_list))
            else:
                pnl_cv.append(np.NaN)

            max_dd_list.append(max_drawdown)
            max_dd_avg.append(np.mean(max_dd_list))
            max_dd_std.append(np.std(max_dd_list))
            if np.mean(max_dd_list) != 0:
                max_dd_cv.append(np.std(max_dd_list)/np.mean(max_dd_list))
            else:
                max_dd_cv.append(np.NaN)

            win_rate_list.append(win_rate)
            win_rate_avg.append(np.mean(win_rate_list))
            win_rate_std.append(np.std(win_rate_list))
            if np.mean(win_rate_list) != 0:
                win_rate_cv.append(np.std(win_rate_list)/np.mean(win_rate_list))
            else:
                win_rate_cv.append(np.NaN)

            mod_win_rate_list.append(mod_win_rate)
            mod_win_rate_avg.append(np.mean(mod_win_rate_list))
            mod_win_rate_std.append(np.std(mod_win_rate_list))
            if np.mean(mod_win_rate_list) != 0:
                mod_win_rate_cv.append(np.std(mod_win_rate_list)/np.mean(mod_win_rate_list))
            else:
                mod_win_rate_cv.append(np.NaN)

            num_trades_list.append(num_trades)
            num_trades_avg.append(np.mean(num_trades_list))
            num_trades_std.append(np.std(num_trades_list))
            if np.mean(num_trades_list) != 0:
                num_trades_cv.append(np.std(num_trades_list)/np.mean(num_trades_list))
            else:
                num_trades_cv.append(np.NaN)

            max_losses_list.append(max_losses)
            max_losses_avg.append(np.mean(max_losses_list))
            max_losses_std.append(np.std(max_losses_list))
            if np.mean(max_losses_list) != 0:
                max_losses_cv.append(np.std(max_losses_list)/np.mean(max_losses_list))
            else:
                max_losses_cv.append(np.NaN)

            max_wins_list.append(max_wins)
            max_wins_avg.append(np.mean(max_wins_list))
            max_wins_std.append(np.std(max_wins_list))
            if np.mean(max_wins_list) != 0:
                max_wins_cv.append(np.std(max_wins_list)/np.mean(max_wins_list))
            else:
                max_wins_cv.append(np.NaN)

            trades_won_list.append(trades_won)
            trades_won_avg.append(np.mean(trades_won_list))
            trades_won_std.append(np.std(trades_won_list))
            if np.mean(trades_won_list) != 0:
                trades_won_cv.append(np.std(trades_won_list) / np.mean(trades_won_list))
            else:
                trades_won_cv.append(np.NaN)

            trades_lost_list.append(trades_lost)
            trades_lost_avg.append(np.mean(trades_lost_list))
            trades_lost_std.append(np.std(trades_lost_list))
            if np.mean(trades_lost_list) != 0:
                trades_lost_cv.append(np.std(trades_lost_list) / np.mean(trades_lost_list))
            else:
                trades_lost_cv.append(np.NaN)

            breakeven_trades_list.append(breakeven_trades)
            breakeven_trades_avg.append(np.mean(breakeven_trades_list))
            breakeven_trades_std.append(np.std(breakeven_trades_list))
            if np.mean(breakeven_trades_list) != 0:
                breakeven_trades_cv.append(np.std(breakeven_trades_list) / np.mean(breakeven_trades_list))
            else:
                breakeven_trades_cv.append(np.NaN)

            profit_factor_list.append(profit_factor)
            profit_factor_avg.append(np.mean(profit_factor_list))
            profit_factor_std.append(np.std(profit_factor_list))
            if np.mean(profit_factor_list) != 0:
                profit_factor_cv.append(np.std(profit_factor_list) / np.mean(profit_factor_list))
            else:
                profit_factor_cv.append(np.NaN)

            from_time_timestamps.append(from_time + (60 * TF_SECONDS[tf]))
            start = datetime.datetime.fromtimestamp(from_time + (60 * TF_SECONDS[tf]))
            start = start.strftime("%Y-%m-%d-%I:%M%p")
            end = datetime.datetime.fromtimestamp(to_time)
            end = end.strftime("%Y-%m-%d-%I:%M%p")
            from_time_list.append(start)
            to_time_list.append(end)
            # return pnl, max_drawdown, win_rate, rr_long, rr_short, num_trades, max_losses, max_wins
            if mode == "s":
                print(f"\r{num_results} results complete. Test {i+1} of {tests}", end=" ")
            # else:
            #     print(f"\rParameter set #{iteration+1} Test {i+1} of {tests}", end=" ")

        i += 1

    df['from_time'] = from_time_list
    df['to_time'] = to_time_list

    df['pnl'] = pnl_list
    df['pnl_avg'] = pnl_avg
    df['pnl_std'] = pnl_std
    df['pnl_cv'] = pnl_cv
    df['%_positive'] = pnl_percent_pos

    df['max_dd'] = max_dd_list
    df['max_dd_avg'] = max_dd_avg
    df['max_dd_std'] = max_dd_std
    df['max_dd_cv'] = max_dd_cv

    df['win_rate'] = win_rate_list
    df['win_rate_avg'] = win_rate_avg
    df['win_rate_std'] = win_rate_std
    df['win_rate_cv'] = win_rate_cv

    df['mod_win_rate'] = mod_win_rate_list
    df['mod_win_rate_avg'] = mod_win_rate_avg
    df['mod_win_rate_std'] = mod_win_rate_std
    df['mod_win_rate_cv'] = mod_win_rate_cv

    df['num_trades'] = num_trades_list
    df['num_trades_avg'] = num_trades_avg
    df['num_trades_std'] = num_trades_std
    df['num_trades_cv'] = num_trades_cv

    df['trades_won'] = trades_won_list
    df['trades_won_avg'] = trades_won_avg
    df['trades_won_std'] = trades_won_std
    df['trades_won_cv'] = trades_won_cv

    df['trades_lost'] = trades_lost_list
    df['trades_lost_avg'] = trades_lost_avg
    df['trades_lost_std'] = trades_lost_std
    df['trades_lost_cv'] = trades_lost_cv

    df['breakeven_trades'] = breakeven_trades_list
    df['breakeven_trades_avg'] = breakeven_trades_avg
    df['breakeven_trades_std'] = breakeven_trades_std
    df['breakeven_trades_cv'] = breakeven_trades_cv

    df['max_wins'] = max_wins_list
    df['max_wins_avg'] = max_wins_avg
    df['max_wins_std'] = max_wins_std
    df['max_wins_cv'] = max_wins_cv

    df['max_losses'] = max_losses_list
    df['max_losses_avg'] = max_losses_avg
    df['max_losses_std'] = max_losses_std
    df['max_losses_cv'] = max_losses_cv

    df['profit_factor'] = profit_factor_list
    df['profit_factor_avg'] = profit_factor_avg
    df['profit_factor_std'] = profit_factor_std
    df['profit_factor_cv'] = profit_factor_cv

    df['rr_long'] = rr_long
    df['rr_short'] = rr_short
    df.index += 1

    return df
