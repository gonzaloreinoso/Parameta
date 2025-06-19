#!/usr/bin/env python3
"""
Compute rolling standard deviation for bid, mid, and ask prices per security ID
using an incremental window algorithm (process A) for contiguous hourly snaps.

Usage:
    python stdev_rolling.py

Make sure the input file `stdev_price_data.parq.gzip` is in the data directory.
The output CSV will be written to `stdev_std_output.csv` in the results directory.
"""
import pandas as pd
import numpy as np
from collections import deque
import os
import time  # Added for timing functionality


class RollingStdCalculator:
    def __init__(self, window_size: int = 20):
        self.window_size = window_size

    def calculate(self,
                  input_path: str,
                  output_path: str,
                  start_date: str,
                  end_date: str) -> None:
        # Load data
        df = pd.read_parquet(input_path, engine='pyarrow')
        df['timestamp'] = pd.to_datetime(df['snap_time'])        # Include seven days before start to seed rolling window
        lookback_start = pd.to_datetime(start_date) - pd.Timedelta(days=1)
        df = df[df['timestamp'] >= lookback_start]
        df = df.sort_values(['security_id', 'timestamp'])
        
        # Remove rows with missing values in bid, mid, or ask columns
        df = df.dropna(subset=['bid', 'mid', 'ask'])

        results = []
        for sec_id, group in df.groupby('security_id', sort=False):
            results.append(self._process_group(sec_id, group))

        out_df = pd.concat(results, ignore_index=True)
        out_df = out_df[(out_df['timestamp'] >= pd.to_datetime(start_date)) &
                        (out_df['timestamp'] <= pd.to_datetime(end_date))]
        out_df.to_csv(output_path, index=False)
        
    def _process_group(self, sec_id: str, group: pd.DataFrame) -> pd.DataFrame:
        window = deque()
        sum_bid = sum_mid = sum_ask = 0.0
        sumsq_bid = sumsq_mid = sumsq_ask = 0.0
        last_ts = None

        records = []
        for row in group.itertuples(index=False):
            ts = row.timestamp
            # reset on gap
            if last_ts is None or ts - last_ts != pd.Timedelta(hours=1):
                window.clear()
                sum_bid = sum_mid = sum_ask = 0.0
                sumsq_bid = sumsq_mid = sumsq_ask = 0.0            # Get bid, mid, ask values and ensure they're floating point
            b, m, a = float(row.bid), float(row.mid), float(row.ask)
            
            # add to window
            window.append((b, m, a))
            
            # add to running sums
            sum_bid += b
            sumsq_bid += b * b
            sum_mid += m
            sumsq_mid += m * m
            sum_ask += a
            sumsq_ask += a * a
            
            # drop oldest
            if len(window) > self.window_size:
                old_b, old_m, old_a = window.popleft()
                sum_bid -= old_b
                sumsq_bid -= old_b * old_b
                sum_mid -= old_m
                sumsq_mid -= old_m * old_m
                sum_ask -= old_a
                sumsq_ask -= old_a * old_a

            # compute standard deviations when window is full
            if len(window) == self.window_size:
                n = self.window_size
                
                # Calculate variances with population formula
                var_bid = (sumsq_bid - (sum_bid * sum_bid) / n) / n
                var_mid = (sumsq_mid - (sum_mid * sum_mid) / n) / n
                var_ask = (sumsq_ask - (sum_ask * sum_ask) / n) / n
                
                # Fix for negative variance due to floating point errors
                var_bid = max(0, var_bid)
                var_mid = max(0, var_mid)
                var_ask = max(0, var_ask)
                
                # Calculate standard deviations
                sd_bid = np.sqrt(var_bid)
                sd_mid = np.sqrt(var_mid)
                sd_ask = np.sqrt(var_ask)
                
                # Only add the record if we have valid standard deviation values
                if not (np.isnan(sd_bid) and np.isnan(sd_mid) and np.isnan(sd_ask)):
                    records.append({
                        'security_id': sec_id,
                        'timestamp': ts,
                        'stdev_bid': sd_bid,
                        'stdev_mid': sd_mid,
                        'stdev_ask': sd_ask
                    })
            # We don't append records when window size is not met

            last_ts = ts

        return pd.DataFrame.from_records(records)


def main():
    # Start timer
    start_time = time.time()
    
    # Define base paths relative to script location
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    BASE_DIR = os.path.dirname(SCRIPT_DIR)  # stdev_test directory
    
    # Define file paths
    INPUT_FILE = os.path.join(BASE_DIR, 'data', 'stdev_price_data.parq.gzip')
    OUTPUT_FILE = os.path.join(BASE_DIR, 'results', 'stdev_a.csv')
    START_TS = '2021-11-20T00:00:00'
    END_TS   = '2021-11-23T09:00:00'

    calc = RollingStdCalculator(window_size=20)
    calc.calculate(
        input_path=INPUT_FILE,
        output_path=OUTPUT_FILE,
        start_date=START_TS,
        end_date=END_TS
    )
    
    # End timer and calculate elapsed time
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    print(f"Wrote rolling standard deviations to '{OUTPUT_FILE}'")
    print(f"Execution time: {elapsed_time:.2f} seconds")


if __name__ == '__main__':
    main()
