#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import time
import sys
import pathlib

# Load stock price data from pre-downloaded CSV files
apple_stock_data = pd.read_csv('AAPL_4_years_data.csv')
microsoft_stock_data = pd.read_csv('MSFT_4_years_data.csv')

# Add a 'Symbol' column to distinguish the stocks
apple_stock_data['Symbol'] = 'AAPL'
microsoft_stock_data['Symbol'] = 'MSFT'

# Print system configuration information for debugging
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

# Extract dates from Apple stock data
date_column = apple_stock_data['datetime']
most_recent_date = date_column.iloc[-1]
oldest_date = date_column.iloc[0]

# Stream delay and interval parameters
initial_delay_seconds = 30
data_stream_interval = 1  # Interval between streaming data points (in seconds)

# Scale Microsoft data to match Apple's closing price on the most recent date
scaling_factor = (
    apple_stock_data.loc[apple_stock_data['datetime'] == most_recent_date, 'close'].values[0] /
    microsoft_stock_data.loc[microsoft_stock_data['datetime'] == most_recent_date, 'close'].values[0]
)
microsoft_stock_data['close'] *= scaling_factor

# Merge the dataframes on the 'datetime' column
merged_stock_data = pd.merge(
    apple_stock_data, microsoft_stock_data, on='datetime', suffixes=('_aapl', '_msft')
)

# Filter the merged dataset to include only rows with time `15:45:00`
merged_stock_data['time'] = pd.to_datetime(merged_stock_data['datetime']).dt.time
filtered_stock_data = merged_stock_data[merged_stock_data['time'] == pd.Timestamp('15:45:00').time()]

# Save the filtered data for reference
filtered_stock_data.to_csv('filtered_merged_stock_data.csv', encoding='utf-8', index=False)

# Main script logic for data streaming
if __name__ == '__main__':
    print(
        f'Streaming daily prices for AAPL and MSFT from {oldest_date[:10]} to {most_recent_date[:10]}...',
        flush=True, file=sys.stderr
    )
    print(f'Each day\'s data will be sent every {data_stream_interval} seconds.', flush=True, file=sys.stderr)
    print(f'Stream will begin in {initial_delay_seconds} seconds.', flush=True, file=sys.stderr)
    print(
        f'MSFT prices have been scaled to match AAPL\'s price on {most_recent_date[:10]}.',
        flush=True, file=sys.stderr
    )

    # Simulate an initial delay before streaming begins
    from tqdm import tqdm
    for second in tqdm(range(initial_delay_seconds), desc="Initializing Stream"):
        time.sleep(0.5)

    # Stream data row by row
    for _, row in filtered_stock_data.iterrows():
        # Stream format: date, AAPL closing price, MSFT closing price
        print(
            f"{row['datetime'][:10]}\t{row['close_aapl']:.4f}\t{row['close_msft']:.4f}",
            flush=True
        )
        time.sleep(data_stream_interval)

    print("Data streaming completed successfully.", flush=True, file=sys.stderr)
    exit(0)
