import typing

class Contract:
    def __init__(self, contract_info):
        self.symbol = contract_info['name']
        self.base_currency = contract_info['base_currency']
        self.quote_currency = contract_info['quote_currency']
        self.leverage_filter = contract_info['leverage_filter']
        self.price_filter = contract_info['price_filter']
        self.lot_size_filter = contract_info['lot_size_filter']

class Candle:
    def __init__(self, candle_info):
        if 'start_at' in candle_info:
            self.start = int(candle_info['start_at'])
            self.open = float(candle_info['open'])
            self.high = float(candle_info['high'])
            self.low = float(candle_info['low'])
            self.close = float(candle_info['close'])
            self.volume = float(candle_info['volume'])
            # print('historical')
        else:
            # print('live')
            self.start = int(candle_info['start'])
            # self.end = int(candle_info['end'])
            # self.confirm = bool(candle_info['confirm'])
            # self.timestamp = int(candle_info['timestamp'])
            self.open = float(candle_info['open'])
            self.high = float(candle_info['high'])
            self.low = float(candle_info['low'])
            self.close = float(candle_info['close'])
            self.volume = float(candle_info['volume'])

class BacktestResult:
    def __init__(self):
        self.pnl: float = 0.0
        self.max_dd: float = 0.0
        self.parameters: typing.Dict = dict()
        self.dominated_by: int = 0
        self.dominates: typing.List[int] = []
        self.rank: int = 0
        self.crowding_distance: float = 0.0
        self.win_rate: float = 0.0
        self.rr_long: float = 0.0
        self.rr_short: float = 0.0
        self.num_trades: int = 0
        self.mod_win_rate: float = 0.0
        self.max_losses: int = 0
        self.max_wins: int = 0
        self.pnl_dd_ratio: float = 0.0
        self.pnl_cv: float = 0.0
        self.max_dd_cv: float = 0.0
        self.win_rate_cv: float = 0.0
        self.pnl_dd_ratio_cv: float = 0.0
        self.pnl_dd_ratio_history: typing.List[float] = []
        self.pnl_history: typing.List[float] = []
        self.max_dd_history: typing.List[float] = []
        self.win_rate_history: typing.List[float] = []

    def __repr__(self):
        return f"PNL = {round(self.pnl, 2)} Max. Drawdown = {round(self.max_dd, 2)} Parameters  = {self.parameters} " \
               f"Rank = {self.rank} Crowding Distance = {self.crowding_distance}"

    def reset_results(self):
        self.dominated_by = 0
        self.dominates.clear()
        self.rank = 0
        self.crowding_distance = 0.0
        # self.pnl_cv = 0.0
        # self.max_dd_cv = 0.0
        # self.win_rate_cv = 0.0
