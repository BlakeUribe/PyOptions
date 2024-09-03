import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import py_vollib.black_scholes.greeks.analytical as greeks
from py_vollib.black_scholes_merton.implied_volatility import implied_volatility
import pandas_datareader.data as web

# import os
# if not os.getcwd().endswith('PyOptions'):
#     os.chdir('/Users/blakeuribe/Desktop/PyOptions')

# Define a function to calculate days until a target date
def days_until(date_str):
    target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    current_date = datetime.now().date()
    days_until_target = (target_date - current_date).days
    return days_until_target


def get_put_greeks(row):
    # Get today's 1-year Treasury Rate
    today = datetime.today()
    yesterday = today - timedelta(days=5) # Ideally set to 5 to account for weekend and holidays
    treasury_rate_1yr = web.DataReader('DGS1', 'fred', yesterday, today).iloc[-1].values[0]

    T = row['Days_until_exp'] / 365
    r = treasury_rate_1yr / 100  # Risk-free rate (annual)

    # Use scalar values for implied_volatility
    sigma = implied_volatility(row['lastPrice'], row['Stock_price'], row['strike'], T, r, row['Div_yield'], 'p')

    # Calculate the Greeks
    return pd.Series({
        'Delta': greeks.delta('p', row['Stock_price'], row['strike'], T, r, sigma),
        'Gamma': greeks.gamma('p', row['Stock_price'], row['strike'], T, r, sigma),
        'Vega': greeks.vega('p', row['Stock_price'], row['strike'], T, r, sigma),
        'Theta': greeks.theta('p', row['Stock_price'], row['strike'], T, r, sigma),
        'Rho': greeks.rho('p', row['Stock_price'], row['strike'], T, r, sigma),
        'Implied_vol (%)': sigma * 100
    })