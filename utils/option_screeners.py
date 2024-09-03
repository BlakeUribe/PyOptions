import pandas as pd
import yfinance as yf
import requests
from concurrent.futures import ThreadPoolExecutor

import os
if not os.getcwd().endswith('PyOptions'):
    os.chdir('/Users/blakeuribe/Desktop/PyOptions')
    
import sys
sys.path.append('utils') # Possibly fix this?

from option_utils import days_until, get_put_greeks

class NakedPutScreener:
    """
    Initialize the NakedPutScreener class with the given parameters.

    Parameters:
    - stock_symbols (list of str): A list of stock symbols to screen for naked puts. Each symbol should be a valid ticker symbol as used by Yahoo Finance.
    - days_to_expiration (int, optional): The maximum number of days to expiration for the options to be considered. Defaults to 60 days.
    - option_vol (int, optional): The minimum trading volume required for the put options to be included in the screener. Defaults to 100 contracts.
    - open_int (int, optional): The minimum open interest required for the put options to be included in the screener. Defaults to 500 contracts.
    - bottom_delta (float, optional): The minimum delta value for the put options to be included in the final results. Defaults to 0.20.
    - top_delta (float, optional): The maximum delta value for the put options to be included in the final results. Defaults to 0.40.

    Attributes:
    - stock_symbols (list of str): Stores the list of stock symbols provided during initialization.
    - days_to_expiration (int): Stores the maximum number of days to expiration for filtering options.
    - option_vol (int): Stores the minimum trading volume for filtering options.
    - open_int (int): Stores the minimum open interest for filtering options.
    - bottom_delta (float): Stores the minimum delta value for filtering the final options.
    - top_delta (float): Stores the maximum delta value for filtering the final options.
    - final_options_df (pd.DataFrame): DataFrame to store the retrieved options data before filtering.
    - put_screener_df (pd.DataFrame): DataFrame to store the filtered put options data.
    - merged_df (pd.DataFrame): DataFrame to store the final results with calculated Greeks and additional data.
    """

    def __init__(self, stock_symbols, days_to_expiration=60, option_vol=100, open_int=500, bottom_delta=0.20, top_delta=0.40):
        # Initialize the class with list of stock symbols and parameters
        self.stock_symbols = stock_symbols
        self.days_to_expiration = days_to_expiration
        self.option_vol = option_vol
        self.open_int = open_int
        self.bottom_delta = bottom_delta
        self.top_delta = top_delta
        self.final_options_df = pd.DataFrame()
        self.put_screener_df = pd.DataFrame()
        self.merged_df = pd.DataFrame()

    # def get_options_data(self, symbol):
    #     ticker = yf.Ticker(symbol)
    #     options_list = []
    #     try:
    #         expiration_dates = ticker.options
    #         expiration_df = pd.DataFrame({'Expiration_date': expiration_dates})
    #         expiration_df['Days_until_exp'] = expiration_df['Expiration_date'].apply(days_until)
    #         expiration_df = expiration_df[expiration_df['Days_until_exp'] <= self.days_to_expiration]

    #         for expiration_date in expiration_df['Expiration_date']:
    #             try:
    #                 options_chain = ticker.option_chain(expiration_date)
    #                 puts_df = options_chain.puts.drop(columns=['contractSize', 'currency', 'lastTradeDate'])
    #                 puts_df['Expiration_date'] = expiration_date
    #                 puts_df['Symbol'] = symbol
    #                 options_list.append(puts_df)
    #             except ValueError:
    #                 return None

    #         return pd.concat(options_list, ignore_index=True) if options_list else None

    #     except (requests.exceptions.RequestException, ValueError):
    #         return None
        
    def get_options_data(self, symbol):
        try:
            # Ensure the symbol is treated as a string
            ticker = yf.Ticker(str(symbol).upper())  # Convert to string and uppercase
            options_list = []
            try:
                expiration_dates = ticker.options
                expiration_df = pd.DataFrame({'Expiration_date': expiration_dates})
                expiration_df['Days_until_exp'] = expiration_df['Expiration_date'].apply(days_until)
                expiration_df = expiration_df[expiration_df['Days_until_exp'] <= self.days_to_expiration]

                for expiration_date in expiration_df['Expiration_date']:
                    try:
                        options_chain = ticker.option_chain(expiration_date)
                        puts_df = options_chain.puts.drop(columns=['contractSize', 'currency', 'lastTradeDate'])
                        puts_df['Expiration_date'] = expiration_date
                        puts_df['Symbol'] = symbol
                        options_list.append(puts_df)
                    except ValueError:
                        return None

                return pd.concat(options_list, ignore_index=True) if options_list else None

            except (requests.exceptions.RequestException, ValueError) as e:
                return None

        except AttributeError as e:
            return None

    def fetch_options_data(self):
        with ThreadPoolExecutor(max_workers=10) as executor:
            all_options_df_list = list(executor.map(self.get_options_data, self.stock_symbols))

        all_options_df_list = [df for df in all_options_df_list if df is not None]
        self.final_options_df = pd.concat(all_options_df_list, ignore_index=True) if all_options_df_list else pd.DataFrame()

    def filter_put_screener(self):
        self.put_screener_df = self.final_options_df[
            (self.final_options_df['inTheMoney'] == False) &
            (self.final_options_df['volume'] > self.option_vol) &
            (self.final_options_df['openInterest'] > self.open_int)
        ]
        self.put_screener_df = self.put_screener_df.drop(columns=['change', 'inTheMoney', 'impliedVolatility', 'bid', 'percentChange'])
        self.put_screener_df['Days_until_exp'] = self.put_screener_df['Expiration_date'].apply(days_until)

    def fetch_additional_data(self, symbol):
        ticker = yf.Ticker(symbol)
        dividend_yield = ticker.info.get('dividendYield', 0)
        stock_price = ticker.info.get('currentPrice', 0)
        return symbol, dividend_yield, stock_price

    def update_additional_data(self):
        with ThreadPoolExecutor(max_workers=10) as executor:
            additional_data = list(executor.map(self.fetch_additional_data, self.put_screener_df['Symbol'].unique()))

        for symbol, dividend_yield, stock_price in additional_data:
            self.put_screener_df.loc[self.put_screener_df['Symbol'] == symbol, ['Div_yield', 'Stock_price']] = dividend_yield, stock_price

    def calculate_greeks(self):
        greeks_df = self.put_screener_df.apply(get_put_greeks, axis=1)
        self.merged_df = pd.concat([self.put_screener_df, greeks_df], axis=1)
        self.merged_df = self.merged_df[
            (self.merged_df['Delta'].abs() >= self.bottom_delta) & 
            (self.merged_df['Delta'].abs() <= self.top_delta)
        ]
        print(f'Final Screener: Successfully retrieved data for {len(self.merged_df["Symbol"].unique())} out of {len(self.stock_symbols)} stocks.')
        print(f'DataFrame Details: {self.merged_df.shape[0]} rows across {len(self.merged_df["Symbol"].unique())} unique stocks.')

    def to_df(self):
        self.fetch_options_data()
        self.filter_put_screener()
        self.update_additional_data()
        self.calculate_greeks()
        return self.merged_df  # Return the final DataFrame
