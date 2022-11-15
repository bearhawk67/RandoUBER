from bybit import BybitClient
from data_collector import collect_all
from utils import *
import logging.handlers
import time
import datetime
import backtester
import pandas as pd
import os.path
import smtplib
import numpy as np
from configparser import ConfigParser

logger = logging.getLogger()
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s %(levelname)s :: %(message)s")
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.INFO)
file_handler = logging.FileHandler("info.log")
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.INFO)
# logger.addHandler(smtp_handler)
logger.addHandler(stream_handler)
logger.addHandler(file_handler)

parser = ConfigParser()
parser.read('config.ini')
email = parser.get('email', 'email address')


if __name__ == "__main__":
    client = BybitClient()
    modes = ["data", "multitest"]
    while True:
        mode = input("Choose the program mode (data / multitest): ").lower()
        if mode in modes:
            break
    contracts = []
    for a in client.contracts:
        contracts.append(client.contracts[a].symbol)

    while True:
        symbol = input("Choose an asset pair: ").upper()
        if symbol in contracts:
            break

    for i in client.contracts:
        if client.contracts[i].symbol == symbol:
            symbol = client.contracts[i]

    if mode == "data":
        if symbol != "":
            collect_all(client, symbol, 1622413603)
        else:
            for pair in client.usdt_contracts:
                collect_all(client, client.contracts[pair], 1622413603)

    elif mode == "multitest":
        # Strategy
        available_strategies = ["obv", "ichimoku", "sup_res", "mfi", "guppy"]
        while True:
            strategy = input(f"Choose a strategy ({', '.join(available_strategies)}): ").lower()
            if strategy in available_strategies:
                break

        # Timeframe
        while True:
            timeframe = input(f"Choose a timeframe ({', '.join(TF_EQUIV.keys())}): ").lower()
            if timeframe in TF_EQUIV.keys():
                break

        # Data pool
        pool_types = ["full", "last year"]
        while True:
            pool_type = input(f"Test on full history or recent portion? ({', '.join(pool_types)}): ").lower()
            if pool_type in pool_types:
                break

        # Time period length
        while True:
            try:
                days, hours = input("Length of time to backtest (Days Hours separated by space): ").split()
                days = int(days)
                hours = int(hours)
                break
            except ValueError:
                continue

        # Initial Capital
        while True:
            initial_capital = input("Initial capital to trade with or Press Enter for 100: ")
            if initial_capital == "":
                initial_capital = 100
                break
            try:
                initial_capital = int(initial_capital)
                break
            except ValueError:
                continue

        # Number of final results
        while True:
            num_results = input(f"Final number of results required: ")
            try:
                num_results = int(num_results)
                break
            except ValueError:
                continue
        backtester.multitest(symbol, strategy, timeframe, days, hours, initial_capital, num_results,
                             pool_type)

    elif mode in ["backtest", "optimize"]:

        # Strategy
        available_strategies = ["obv", "ichimoku", "sup_res", "mfi", "guppy"]
        while True:
            strategy = input(f"Choose a strategy ({', '.join(available_strategies)}): ").lower()
            if strategy in available_strategies:
                break

        # Timeframe
        while True:
            timeframe = input(f"Choose a timeframe ({', '.join(TF_EQUIV.keys())}): ").lower()
            if timeframe in TF_EQUIV.keys():
                break

        # From
        while True:
            from_time = input("Backtest from (yyyy-mm-dd hh) or Press Enter: ")
            if from_time == "":
                from_time = 0
                break
            try:
                from_time = int(datetime.datetime.strptime(from_time, "%Y-%m-%d %H").timestamp())
                break
            except ValueError:
                continue
        from_time -= (60 * TF_SECONDS[timeframe])

        # To
        while True:
            to_time = input("Backtest until (yyyy-mm-dd hh) or Press Enter for current time: ")
            if to_time == "":
                to_time = time.time()
                break
            try:
                to_time = int(datetime.datetime.strptime(to_time, "%Y-%m-%d %H").timestamp())
                break
            except ValueError:
                continue

        # Initial Capital
        while True:
            initial_capital = input("Initial capital to trade with or Press Enter for 100: ")
            if initial_capital == "":
                initial_capital = int(100)
                break
            try:
                initial_capital = int(initial_capital)
                break
            except ValueError:
                continue

#         if mode == "backtest":
#             backtester.run(symbol, strategy, timeframe, from_time, to_time, initial_capital)
#
#         elif mode == "optimize":
#             # Population size
#             while True:
#                 try:
#                     pop_size = int(input(f"Choose a population size: "))
#                     break
#                 except ValueError:
#                     continue
#
#             # Iterations (generations)
#             while True:
#                 try:
#                     generations = int(input(f"Choose a number of generations: "))
#                     break
#                 except ValueError:
#                     continue
#
#             # RSS tests per generation
#             while True:
#                 try:
#                     tests = int(input(f"Choose a number of random tests per generation: "))
#                     break
#                 except ValueError:
#                     continue
#
#             start_time = time.time()
#             nsga2 = optimizer.Nsga2(symbol, strategy, timeframe, from_time, to_time, initial_capital, pop_size)
#             p_population = nsga2.create_initial_population()
#             p_population = nsga2.evaluate_population(p_population)
#             if tests != 0:
#                 d1 = nsga2.rss_period()
#                 rss_population = nsga2.rss_backtest(d1, p_population)
#                 p_population = nsga2.cv_calculation(p_population, rss_population)
#             p_population = nsga2.crowding_distance(p_population)
#
#             g = 0
#
#             while g < generations:
#                 # Create offspring
#                 q_population = nsga2.create_offspring_population(p_population)
#                 # Evaluate
#                 q_population = nsga2.evaluate_population(q_population)
#                 r_population = p_population + q_population
#
#                 nsga2.population_params.clear()
#
#                 i = 0
#                 population = dict()
#                 for bt in r_population:
#                     bt.reset_results()
#                     nsga2.population_params.append(bt.parameters)
#                     population[i] = bt
#                     i += 1
#
#                 # RSS tests
#                 k = 0
#                 while k < tests:
#                     d1 = nsga2.rss_period()
#                     rss_population = nsga2.rss_backtest(d1, population)
#                     print(f"\r"
#                           f"{int(((g/generations)*100+((k+1)/(generations*tests)*100)))}% "
#                           f" Gen {(g + 1)} Test {k + 1}, RSS period: {d1.index[0]} to {d1.index[-1]} ", end=" ")
#                     k += 1
#
#                 if tests != 0:
#                     population = nsga2.cv_calculation(population, rss_population)
#                 fronts = nsga2.non_dominated_sorting(population)
#
#                 for j in range(len(fronts)):
#                     fronts[j] = nsga2.crowding_distance((fronts[j]))
#
#                 p_population = nsga2.create_new_population(fronts)
#
#                 if tests == 0:
#                     print(f"\r{int((g + 1) / generations * 100)}%", end=" ")
#
#                 g += 1
#
#             print("\n")
#
#             df = pd.DataFrame()
#             # df2 = pd.DataFrame()
#
#             for individual in p_population:
#                 print(individual)
#                 ps = pd.Series(individual.parameters)
#                 ps = pd.DataFrame(ps)
#                 ps = ps.transpose()
#                 # if strategy == "guppy":
#                 #     pr = pd.Series(individual.results)
#                 #     pr = pd.DataFrame(pr)
#                 #     pr = pr.transpose()
#                 #     df2 = pd.concat([df2, pr], axis=1)
#                 ps["pnl"] = individual.pnl
#                 ps["pnl_avg"] = np.mean(individual.pnl_history)
#                 ps["pnl_std"] = np.std(individual.pnl_history)
#                 ps["pnl_cv"] = individual.pnl_cv
#                 ps["max_dd"] = individual.max_dd
#                 ps["max_dd_avg"] = np.mean(individual.max_dd_history)
#                 ps["max_dd_std"] = np.std(individual.max_dd_history)
#                 ps["max_dd_cv"] = individual.max_dd_cv
#                 ps["pnl_dd_ratio"] = individual.pnl_dd_ratio
#                 ps["pnl_dd_ratio_cv"] = individual.pnl_dd_ratio_cv
#                 ps["win_rate"] = individual.win_rate
#                 ps["win_rate_cv"] = individual.win_rate_cv
#                 ps["mod_win_rate"] = individual.mod_win_rate
#                 ps["risk_reward_long"] = individual.rr_long
#                 ps["risk_reward_short"] = individual.rr_short
#                 ps["num_trades"] = individual.num_trades
#                 ps["max_losses"] = individual.max_losses
#                 ps["max_wins"] = individual.max_wins
#                 df = pd.concat([df, ps], axis=0)
#
#             start = datetime.datetime.fromtimestamp(from_time + (60 * TF_SECONDS[timeframe]))
#             start = start.strftime("%Y-%m-%d-%I%p")
#             end = datetime.datetime.fromtimestamp(to_time)
#             end = end.strftime("%Y-%m-%d-%I%p")
#
#             # df2.to_csv("OptimizerResults2.csv")
#             if os.path.exists(f"OptimizerResults_{symbol.symbol}_{timeframe}_"
#                               f"{start}_to_{end}_{pop_size}x{generations}x{tests}.csv"):
#                 while True:
#                     try:
#                         myfile = open(f"OptimizerResults_{symbol.symbol}_{timeframe}_"
#                                       f"{start}_to_{end}_{pop_size}x{generations}x{tests}.csv", "w+")
#                         break
#                     except IOError:
#                         input(f"Cannot write results to csv file. Please close \n"
#                               f"OptimizerResults_{symbol.symbol}_{timeframe}_"
#                               f"{start}_to_{end}_{pop_size}x{generations}x{tests}.csv\nThen press Enter to "
#                               f"retry.")
#             df.to_csv(f"OptimizerResults_{symbol.symbol}_{timeframe}_{start}_to_"
#                       f"{end}_{pop_size}x{generations}x{tests}.csv")
#             try:
#                 smtp = smtplib.SMTP("smtp.gmail.com", 587)
#                 smtp.starttls()
#                 smtp.login("guppy.bot.messenger@gmail.com", "otqcxemvnbxvfjpe")
#                 subject = "BTOM Report"
#                 text = f"{pop_size}x{generations}x{tests} optimization for {symbol.symbol}_{timeframe} from " \
#                        f"{start} to {end} complete."
#                 message = "Subject: {} \n\n {}".format(subject, text)
#                 smtp.sendmail("guppy.bot.messenger@gmail.com", email, message)
#                 smtp.quit()
#             except smtplib.SMTPException as e:
#                 logger.error(e)
#                 smtp = smtplib.SMTP("smtp.gmail.com", 587)
#                 smtp.starttls()
#                 smtp.login("guppy.bot.messenger@gmail.com", "otqcxemvnbxvfjpe")
#                 subject = "BTOM Report"
#                 text = f"{pop_size}x{generations}x{tests} optimization for {symbol.symbol}_{timeframe} from" \
#                        f" {start} to {end} complete."
#                 message = "Subject: {} \n\n {}".format(subject, text)
#                 smtp.sendmail("guppy.bot.messenger@gmail.com", email, message)
#                 smtp.quit()
#
# if mode == "optimize":
#     print(f"Time to complete: {int(time.time() - start_time)} seconds")

a = input("Press Enter to close.")
