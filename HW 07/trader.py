import pandas as pd
import numpy as np

# Load data
apple_stock_data = pd.read_csv('AAPL_4_years_data.csv', parse_dates=['datetime'])
microsoft_stock_data = pd.read_csv('MSFT_4_years_data.csv', parse_dates=['datetime'])

# Sort by datetime
apple_stock_data.sort_values(by='datetime', inplace=True)
microsoft_stock_data.sort_values(by='datetime', inplace=True)

# Scale Microsoft's closing prices to match Apple's most recent closing price
scale_factor = apple_stock_data.iloc[-1]['close'] / microsoft_stock_data.iloc[-1]['close']
microsoft_stock_data['close'] *= scale_factor

# Add suffixes to distinguish between the two datasets
apple_stock_data = apple_stock_data.add_suffix('_aapl')
microsoft_stock_data = microsoft_stock_data.add_suffix('_msft')

# Merge datasets on datetime
merged_data = pd.merge(
    apple_stock_data, 
    microsoft_stock_data, 
    left_on='datetime_aapl', 
    right_on='datetime_msft',
    how='inner'
)

# Drop duplicate datetime columns and rename for clarity
merged_data['datetime'] = merged_data['datetime_aapl']
merged_data.drop(['datetime_aapl', 'datetime_msft'], axis=1, inplace=True)

# Function to calculate moving averages
def calculate_moving_average(data, window):
    return data.rolling(window=window).mean()

# Initialize results list
signals = []

# Process data line by line
merged_data['10_day_ma_aapl'] = calculate_moving_average(merged_data['close_aapl'], 10)
merged_data['40_day_ma_aapl'] = calculate_moving_average(merged_data['close_aapl'], 40)
merged_data['10_day_ma_msft'] = calculate_moving_average(merged_data['close_msft'], 10)
merged_data['40_day_ma_msft'] = calculate_moving_average(merged_data['close_msft'], 40)

# Detect buy and sell signals
for i in range(1, len(merged_data)):
    # Check Apple signals
    if (
        merged_data.iloc[i - 1]['10_day_ma_aapl'] <= merged_data.iloc[i - 1]['40_day_ma_aapl'] and
        merged_data.iloc[i]['10_day_ma_aapl'] > merged_data.iloc[i]['40_day_ma_aapl']
    ):
        signals.append((merged_data.iloc[i]['datetime'], 'AAPL', 'buy'))
    elif (
        merged_data.iloc[i - 1]['10_day_ma_aapl'] >= merged_data.iloc[i - 1]['40_day_ma_aapl'] and
        merged_data.iloc[i]['10_day_ma_aapl'] < merged_data.iloc[i]['40_day_ma_aapl']
    ):
        signals.append((merged_data.iloc[i]['datetime'], 'AAPL', 'sell'))
    
    # Check Microsoft signals
    if (
        merged_data.iloc[i - 1]['10_day_ma_msft'] <= merged_data.iloc[i - 1]['40_day_ma_msft'] and
        merged_data.iloc[i]['10_day_ma_msft'] > merged_data.iloc[i]['40_day_ma_msft']
    ):
        signals.append((merged_data.iloc[i]['datetime'], 'MSFT', 'buy'))
    elif (
        merged_data.iloc[i - 1]['10_day_ma_msft'] >= merged_data.iloc[i - 1]['40_day_ma_msft'] and
        merged_data.iloc[i]['10_day_ma_msft'] < merged_data.iloc[i]['40_day_ma_msft']
    ):
        signals.append((merged_data.iloc[i]['datetime'], 'MSFT', 'sell'))

# Create a DataFrame for the results
signals_df = pd.DataFrame(signals, columns=["datetime", "symbol", "signal"])

# Print the head of the results
print(signals_df.head(10).to_string(index=False))
