import pandas as pd
import numpy as np
from pathlib import Path
import time

class RollingStdevCalculator:
    def __init__(self, price_path):
        self.price_path = Path(price_path)
        self.df = None

    def load_data(self):
        # Load and preprocess data efficiently
        self.df = pd.read_parquet(self.price_path)
        self.df['snap_time'] = pd.to_datetime(self.df['snap_time'])
        # Pre-sort the data to avoid multiple sorts later
        self.df.sort_values(['security_id', 'snap_time'], inplace=True)
        
    def process(self, start, end, window=20):
        start_dt = pd.to_datetime(start)
        end_dt = pd.to_datetime(end)
        
        # Filter the relevant time range plus lookback period
        lookback_start = start_dt - pd.Timedelta(days=7)
        mask = (self.df['snap_time'] >= lookback_start) & (self.df['snap_time'] <= end_dt)
        working_df = self.df[mask].copy()
        
        # Create an hour-based index for checking contiguous periods
        working_df['hour_number'] = (working_df['snap_time'] - working_df['snap_time'].min()).dt.total_seconds() / 3600
        
        results = []
        
        # Group by security_id for vectorized operations
        for sec_id, group in working_df.groupby('security_id'):
            # Create a continuous hour range for this security
            hour_idx = pd.DataFrame(
                index=pd.date_range(start=group['snap_time'].min(), 
                                  end=group['snap_time'].max(), 
                                  freq='h')
            )
            
            # Merge with actual data to identify gaps
            merged = hour_idx.merge(group, left_index=True, right_on='snap_time', how='left')
            
            # Find contiguous sequences
            merged['gap'] = merged['hour_number'].isna()
            merged['gap_group'] = merged['gap'].cumsum()
            
            valid_data = merged[~merged['gap']]
            
            # Process each contiguous sequence
            for _, sequence in valid_data.groupby('gap_group'):
                if len(sequence) < window:
                    continue
                    
                # Calculate rolling standard deviations efficiently
                rolling_data = sequence[['bid', 'mid', 'ask']].rolling(
                    window=window,
                    min_periods=window
                ).std(ddof=0)
                
                # Filter for the requested date range
                mask = (sequence['snap_time'] >= start_dt) & (sequence['snap_time'] <= end_dt)
                if not mask.any():
                    continue
                    
                result_rows = pd.DataFrame({
                    'security_id': sec_id,
                    'snap_time': sequence['snap_time'][mask],
                    'bid_stdev': rolling_data['bid'][mask],
                    'mid_stdev': rolling_data['mid'][mask],
                    'ask_stdev': rolling_data['ask'][mask]
                }).dropna()
                
                results.append(result_rows)
        
        if not results:
            return pd.DataFrame()
            
        return pd.concat(results, ignore_index=True).sort_values(['security_id', 'snap_time'])

    def save(self, df, output_path):
        df.to_csv(output_path, index=False)

if __name__ == '__main__':
    base = Path(__file__).resolve().parents[1]
    price = base / 'data/stdev_price_data.parq.gzip'
    out = base / 'results/stdev_rolling.csv'
    
    calc = RollingStdevCalculator(price)
    calc.load_data()
    
    start = time.perf_counter()
    result = calc.process('2021-11-20 00:00:00', '2021-11-23 09:00:00')
    end = time.perf_counter()
    
    calc.save(result, out)
    print(f"Processing time: {end - start:.4f} seconds")
    print(f'Output written to {out}')
