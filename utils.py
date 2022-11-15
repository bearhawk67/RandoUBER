import datetime
import pandas as pd
from dateutil import tz
from ctypes import *

TF_EQUIV = {"1m": "1Min", "3m": "3Min", "5m": "5Min", "6m": "6Min", "10m": "10Min", "12m": "12Min", "15m": "15Min",
            "20m": "20Min", "30m": "30Min", "1h": "1H", "2h": "2H", "4h": "4H", "6h": "6H", "12h": "12H", "1d": "D"}
TF_SECONDS = {"1m": 60, "3m": 180, "5m": 300, "6m": 360, "10m": 600, "12m": 720, "15m": 900, "20m": 1200,
              "30m": 1800, "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600, "12h": 43200, "1d": 86400}

Y_N = {"y", "Y", "n", "N"}

STRAT_PARAMS = {
    "obv": {
        "ma_period": {"name": "MA Period", "type": int, "min": 2, "max": 200},
    },
    "ichimoku": {
        "kijun": {"name": "Kijun Period", "type": int, "min": 2, "max": 200},
        "tenkan": {"name": "Tenkan Period", "type": int, "min": 2, "max": 200},
    },
    "sup_res": {
        "min_points": {"name": "Min. Points", "type": int, "min": 2, "max": 200},
        "min_diff_points": {"name": "Min. Difference between Points", "type": int, "min": 2, "max": 200},
        "rounding_nb": {"name": "Rounding Number", "type": float, "min": 10, "max": 500, "decimals": 2},
        "take_profit": {"name": "Take Profit %", "type": float, "min": 1, "max": 40, "decimals": 2},
        "stop_loss": {"name": "Stop Loss %", "type": float, "min": 1, "max": 40, "decimals": 2},
    },
    "mfi": {
        "period": {"name": "Period length", "type": int},
        "multiplier": {"name": "Multiplier", "type": float},
        "ypos": {"name": "Y position", "type": float}
    },
    # "sma": {
    #     "slow_ma": {"name": "Slow MA length", "type": int},
    #     "fast_ma": {"name": "Fast MA length", "type": int}
    # },
    "guppy": {
        "trade_longs": {"name": "Trade longs? (Y/N)", "type": Y_N, "choices": ["y", "n"]},
        "trade_shorts": {"name": "Trade shorts? (Y/N)", "type": Y_N, "choices": ["y", "n"]},
        "sl_long": {"name": "SL for longs", "type": float, "min": 0.1, "max": 2.5, "decimals": 1},
        "sl_short": {"name": "SL for shorts", "type": float, "min": 0.1, "max": 2.5, "decimals": 1},
        "mfi_long": {"name": "Use MFI for longs? (Y/N)", "type": Y_N, "choices": ["y", "n"]},
        "mfi_short": {"name": "Use MFI for shorts? (Y/N)", "type": Y_N, "choices": ["y", "n"]},
        "mfi_period": {"name": "MFI Period", "type": int, "min": 2, "max": 200},
        "mfi_mult": {"name": "MFI area multiplier", "type": int, "min": 2, "max": 500},
        "mfi_ypos": {"name": "MFI ypos", "type": float, "min": 1.0, "max": 10.0, "decimals": 1},
        "mfi_long_threshold": {"name": "MFI threshold for longs", "type": float, "min": 0.0, "max": 100.0,
                               "decimals": 1},
        "mfi_short_threshold": {"name": "MFI threshold for shorts", "type": float, "min": -100.0, "max": 0.0,
                                "decimals": 1},
        "macd_long": {"name": "Use MACD for longs? (Y/N)", "type": Y_N, "choices": ["y", "n"]},
        "macd_short": {"name": "Use MACD for shorts (Y/N)", "type": Y_N, "choices": ["y", "n"]},
        "macd_fast": {"name": "MACD fast length", "type": int, "min": 2, "max": 200},
        "macd_slow": {"name": "MACD slow length", "type": int, "min": 3, "max": 201},
        "macd_signal": {"name": "MACD signal length", "type": int, "min": 2, "max": 200},
        "rsi_long": {"name": "Use RSI for longs? (Y/N)", "type": Y_N, "choices": ["y", "n"]},
        "rsi_short": {"name": "Use RSI for shorts (Y/N)", "type": Y_N, "choices": ["y", "n"]},
        "rsi_length": {"name": "RSI length", "type": int, "min": 2, "max": 200},
        "rsi_overbought": {"name": "RSI overbought value", "type": int, "min": 50, "max": 99},
        "rsi_oversold": {"name": "RSI oversold value", "type": int, "min": 1, "max": 50},
        "adx_long": {"name": "Use ADX for longs? (Y/N)", "type": Y_N, "choices": ["y", "n"]},
        "adx_short": {"name": "Use ADX for shorts (Y/N)", "type": Y_N, "choices": ["y", "n"]},
        "adx_smoothing": {"name": "ADX smoothing", "type": int, "min": 5, "max": 100},
        "adx_di_length": {"name": "ADX DI length", "type": int, "min": 10, "max": 100},
        "adx_length_long": {"name": "ADX length for longs", "type": float, "min": 10.0, "max": 100.0, "decimals": 1},
        "adx_length_short": {"name": "ADX length for shorts", "type": float, "min": 10.0, "max": 100.0, "decimals": 1},
        "bb_long": {"name": "Use Bollinger Bands for longs? (Y/N)", "type": Y_N, "choices": ["y", "n"]},
        "bb_short": {"name": "Use Bollinger Bands for shorts (Y/N)", "type": Y_N, "choices": ["y", "n"]},
        "bb_length": {"name": "Bollinger Band length", "type": int, "min": 5, "max": 200},
        "bb_mult": {"name": "Bollinger Band Standard Dev. Multiplier", "type": float, "min": 1.0,
                    "max": 5.0, "decimals": 1},
        "wae_long": {"name": "Use WAE for longs? (Y/N)", "type": Y_N, "choices": ["n"]},
        "wae_short": {"name": "Use WAE for shorts (Y/N)", "type": Y_N, "choices": ["n"]},
        "wae_sensitivity": {"name": "WAE sensitivity", "type": int, "min": 150, "max": 150},
        "wae_fast_length": {"name": "WAE Fast EMA length", "type": int, "min": 20, "max": 20},
        "wae_slow_length": {"name": "WAE Slow EMA length", "type": int, "min": 40, "max": 40},
        "wae_bb_length": {"name": "WAE Bollinger Band length", "type": int, "min": 20, "max": 20},
        "wae_bb_mult": {"name": "WAE Bollinger Band Standard Dev. Multiplier", "type": float, "min": 2.0,
                        "max": 2.0, "decimals": 1},
        "wae_rma_length": {"name": "WAE Deadzone RMA length", "type": int, "min": 100, "max": 100},
        "wae_dz_mult": {"name": "WAE Deadzone multiplier", "type": float, "min": 3.8, "max": 3.8, "decimals": 1},
        "wae_expl_check": {"name": "Only trade if explosion line is over Deadzone (Y/N)", "type": Y_N,
                           "choices": ["n"]},
        "ema200_long": {"name": "Use 200 EMA for longs? (Y/N)", "type": Y_N, "choices": ["y", "n"]},
        "ema200_short": {"name": "Use 200 EMA for shorts? (Y/N)", "type": Y_N, "choices": ["y", "n"]},
        "guppy_fast_long": {"name": "Use guppy fast ribbon for longs? (Y/N)", "type": Y_N, "choices": ["y", "n"]},
        "guppy_fast_short": {"name": "Use guppy fast ribbon for shorts? (Y/N)", "type": Y_N, "choices": ["y", "n"]},
        "ribbon_check_long": {"name": "Don't enter longs unless price closes above: [Fast Band (1-6), Slow Band (7-12),"
                                      " Off (13)]", "type": int, "min": 1, "max": 13},
        "ribbon_check_short": {"name": "Don't enter shorts unless price closes below: [Fast Band (1-6), Slow Band ("
                                       "7-12), Off (13)]", "type": int, "min": 1, "max": 13},
        "move_sl_type_long": {"name": "When to move SL on longs (1. After set %, 2. After TP1, 3. After TP2, 4. Off)",
                              "type": int, "min": 1, "max": 2},
        "move_sl_type_short": {"name": "When to move SL on shorts (1. After set %, 2. After TP1, 3. After TP2, 4. Off)",
                               "type": int, "min": 1, "max": 2},
        "move_sl_long": {"name": "Move SL to breakeven when price moves __% (longs)", "type": float, "min": 0.2,
                         "max": 3.0, "decimals": 1},
        "move_sl_short": {"name": "Move SL to breakeven when price moves __% (shorts)", "type": float, "min": 0.2,
                          "max": 3.0, "decimals": 1},
        "risk": {"name": "Percent of balance to risk (1-5%) or 6. Trade with 100% and manually set leverage",
                 "type": int, "min": 1, "max": 5},
        "leverage": {"name": "Leverage to use if selected 6 in previous setting.", "type": float, "min": 1,
                     "max": 1, "decimals": 1},
        "tp_long": {"name": "Number of TPs on longs (Max 3)", "type": int, "min": 1, "max": 1},
        "tp_short": {"name": "Number of TPs on shorts (Max 3)", "type": int, "min": 1, "max": 1},
        "ltp1": {"name": "TP1 (%) for longs", "type": float, "min": 0.3, "max": 10.0, "decimals": 1},
        "ltp1_qty": {"name": "% of trade to close at TP1 (long)", "type": float, "min": 0.1, "max": 100.0,
                     "decimals": 1},
        "ltp2": {"name": "TP2 (%) for longs", "type": float, "min": 1, "max": 1, "decimals": 1},
        "ltp2_qty": {"name": "% of trade to close at TP2 (long)", "type": float, "min": 1, "max": 1,
                     "decimals": 1},
        "ltp3": {"name": "TP3 (%) for longs", "type": float, "min": 1, "max": 1, "decimals": 1},
        "stp1": {"name": "TP1 (%) for shorts", "type": float, "min": 0.3, "max": 10.0, "decimals": 1},
        "stp1_qty": {"name": "% of trade to close at TP1 (short)", "type": float, "min": 0.1, "max": 100.0,
                     "decimals": 1},
        "stp2": {"name": "TP2 (%) for shorts", "type": float, "min": 1, "max": 1, "decimals": 1},
        "stp2_qty": {"name": "% of trade to close at TP2 (short)", "type": float, "min": 1, "max": 1,
                     "decimals": 1},
        "stp3": {"name": "TP3 (%) for shorts", "type": float, "min": 1, "max": 1, "decimals": 1},
    }
}


def sec_to_dt(s: int) -> datetime.datetime:
    from_zone = tz.tzutc()
    to_zone = tz.tzlocal()
    dt = datetime.datetime.utcfromtimestamp(s)
    dt.replace(tzinfo=from_zone)
    local_dt = dt.astimezone(to_zone)
    return local_dt


def resample_timeframe(data: pd.DataFrame, tf: str) -> pd.DataFrame:
    return data.resample(TF_EQUIV[tf]).agg({"open": "first", "high": "max", "low": "min", "close": "last",
                                            "volume": "sum"})


def get_library():
    # import C++ library
    lib = CDLL("backtestingCpp/build/libbacktestingCpp.dll", winmode=0)

    # Always indicate the argtypes and restype!
    lib.Sma_new.restype = c_void_p
    lib.Sma_new.argtypes = [c_char_p, c_char_p, c_char_p, c_longlong, c_longlong]

    lib.Sma_execute_backtest.restype = c_void_p
    lib.Sma_execute_backtest.argtypes = [c_void_p, c_int, c_int]

    lib.Sma_get_pnl.restype = c_double
    lib.Sma_get_pnl.argtypes = [c_void_p]

    lib.Sma_get_max_dd.restype = c_double
    lib.Sma_get_max_dd.argtypes = [c_void_p]

    return lib
