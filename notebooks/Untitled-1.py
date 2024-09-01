
import pandas as pd
import yfinance as yf
import os
from datetime import datetime, timedelta
import numpy as np

import py_vollib.black_scholes.greeks.analytical as greeks
from py_vollib.black_scholes_merton.implied_volatility import implied_volatility
import pandas_datareader.data as web
import pandas as pd 

# Define a function to calculate days until a target date
def days_until(date_str):
    target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    current_date = datetime.now().date()
    days_until_target = (target_date - current_date).days
    return days_until_target


def get_greeks(row):
    # Get today's 1-year Treasury Rate
    today = datetime.today()
    yesterday = today - timedelta(days=3)
    treasury_rate_1yr = web.DataReader('DGS1', 'fred', yesterday, today).iloc[-1].values[0]

    T = row['days_until'] / 365
    r = treasury_rate_1yr / 100  # Risk-free rate (annual)

    # Use scalar values for implied_volatility
    sigma = implied_volatility(row['lastPrice'], row['Stock_price'], row['strike'], T, r, 0, 'p')

    # Calculate the Greeks
    return pd.Series({
        'Delta': greeks.delta('p', row['Stock_price'], row['strike'], T, r, sigma),
        'Gamma': greeks.gamma('p', row['Stock_price'], row['strike'], T, r, sigma),
        'Vega': greeks.vega('p', row['Stock_price'], row['strike'], T, r, sigma),
        'Theta': greeks.theta('p', row['Stock_price'], row['strike'], T, r, sigma),
        'Rho': greeks.rho('p', row['Stock_price'], row['strike'], T, r, sigma),
        'Implied_vol (%)': sigma * 100
    })


if os.getcwd() != '/Users/blakeuribe/Desktop/PyOptions':
    os.chdir('..') 

nyse_symbols = pd.read_csv('data/raw_data/nasdaq_screener_1725126512148.csv')['Symbol'].unique()

#  Global 
days_to_expiration = 60 #less than
option_vol = 100 #greater than 
open_int = 500 #less than 
option_type = 'p'

# delta = [0.20, 0.40] #between 


for symbol in nyse_symbols:
# symbol = 'atha'
    ticker = yf.Ticker(symbol)

    # Create a DataFrame with expiration dates
    expiration_df = pd.DataFrame({'expiration_date': ticker.options})

    # Apply the function to each expiration date in the DataFrame
    expiration_df['days_until'] = expiration_df['expiration_date'].apply(days_until)
    expiration_df = expiration_df[expiration_df['days_until'] <= days_to_expiration]
    
    options_list = []

    for expiration_date in list(expiration_df['expiration_date']):
        options_chain = ticker.option_chain(expiration_date)
        
        calls_df = options_chain.calls
        puts_df = options_chain.puts
        
        calls_df = calls_df.drop(columns=['contractSize', 'currency', 'lastTradeDate', 'contractSymbol'])
        puts_df = puts_df.drop(columns=['contractSize', 'currency', 'lastTradeDate', 'contractSymbol'])
        
        calls_df['expiration_date'] = expiration_date
        puts_df['expiration_date'] = expiration_date
        
        calls_df['Type'] = 'Call'
        puts_df['Type'] = 'Put'
        
        options_list.append(calls_df)
        options_list.append(puts_df)


    options_df = pd.concat(options_list, ignore_index=True)
    options_df.insert(0, 'Symbol', symbol)

    column_to_move = options_df.pop('expiration_date')
    options_df.insert(1, 'expiration_date', column_to_move)

    # Create naked put screener
    # Basic filters
    put_screener_df = options_df[
        (options_df['Type'] == 'Put') & 
        (options_df['inTheMoney'] == False)
        ]

    # numerical filter
    put_screener_df = put_screener_df[
        (put_screener_df['volume'] > option_vol) &
        (put_screener_df['openInterest'] > open_int) 
        ]

    cols_to_drop_put = [
        'change',
        'inTheMoney',
        'Type',
        'impliedVolatility',
        'bid',
        'percentChange'
        ]

    put_screener_df = put_screener_df.drop(columns=cols_to_drop_put)
    put_screener_df['days_until'] = put_screener_df['expiration_date'].apply(days_until)
    put_screener_df['Stock_price'] = ticker.history()['Close'].iloc[-1]




    greeks_df = put_screener_df.apply(get_greeks, axis=1)

    # Combine with the original DataFrame
    result_df = pd.concat([put_screener_df, greeks_df], axis=1)
    result_df