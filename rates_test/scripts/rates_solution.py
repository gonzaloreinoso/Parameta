import pandas as pd
import numpy as np
from pathlib import Path
import time

class RatesPriceConverter:
    def __init__(self, ccy_path, price_path, spot_path):
        self.ccy_path = Path(ccy_path)
        self.price_path = Path(price_path)
        self.spot_path = Path(spot_path)
        self.ccy_df = None
        self.price_df = None
        self.spot_df = None

    def load_data(self):
        self.ccy_df = pd.read_csv(self.ccy_path)
        self.price_df = pd.read_parquet(self.price_path)
        self.spot_df = pd.read_parquet(self.spot_path)
        self.price_df['timestamp'] = pd.to_datetime(self.price_df['timestamp'])
        self.spot_df['timestamp'] = pd.to_datetime(self.spot_df['timestamp'])

    def process(self):
        # Merge price with ccy info
        merged = self.price_df.merge(self.ccy_df, on='ccy_pair', how='left', indicator=True)
        
        # Create a unique row identifier, even for duplicates
        base_row_id = merged['ccy_pair'].astype(str) + '|' + merged['timestamp'].astype(str)
        # Use cumcount to append suffix for duplicates
        dup_count = merged.groupby(base_row_id).cumcount()
        merged['row_id'] = base_row_id + '_' + (dup_count + 1).astype(str)
        
        # Initialize result columns
        merged['new_price'] = merged['price']  # Default to original price
        merged['reason'] = 'no conversion required'  # Default reason
        
        # Mark rows where conversion is not possible
        invalid_mask = (merged['_merge'] == 'left_only') | pd.isna(merged['convert_price'])
        merged.loc[invalid_mask, 'reason'] = 'ccy_pair not supported or missing conversion info'
        merged.loc[invalid_mask, 'new_price'] = np.nan
        
        # Get rows that need conversion
        conversion_needed = merged[
            (~invalid_mask) & 
            (merged['convert_price'] == True)
        ].copy()
        
        if not conversion_needed.empty:
            # Calculate previous hour for each timestamp
            conversion_needed['prev_hour'] = conversion_needed['timestamp'].dt.floor('h') - pd.Timedelta(hours=1)
            
            # Prepare spot rates data
            spot_df = self.spot_df.copy()
            
            # Create a window for each price point
            conversion_window = pd.merge_asof(
                conversion_needed.sort_values('timestamp'),
                spot_df.sort_values('timestamp'),
                by='ccy_pair',
                left_on='timestamp',
                right_on='timestamp',
                direction='backward'
            )
    
            # Apply conversion only where we have valid spot rates within the previous hour
            valid_conversions = (
                conversion_window['timestamp'] - 
                conversion_window['timestamp'].dt.floor('h')
            ).dt.total_seconds() <= 3600  # 1 hour in seconds
            
            # Calculate new prices where we have valid spot rates
            conversion_window.loc[valid_conversions, 'new_price'] = (
                conversion_window.loc[valid_conversions, 'price'] / 
                conversion_window.loc[valid_conversions, 'conversion_factor']
            ) + conversion_window.loc[valid_conversions, 'spot_mid_rate']
            
            conversion_window.loc[valid_conversions, 'reason'] = 'converted'
            conversion_window.loc[~valid_conversions, 'reason'] = 'no spot_mid_rate in previous hour'
            conversion_window.loc[~valid_conversions, 'new_price'] = np.nan
            
            # Set row_id as index for both DataFrames
            merged.set_index('row_id', inplace=True)
            conversion_window.set_index('row_id', inplace=True)
            # Update the original merged dataframe with conversion results using row_id
            merged.loc[conversion_window.index, ['new_price', 'reason']] = conversion_window[['new_price', 'reason']]
            # Reset index to restore row_id as a column
            merged.reset_index(drop=True, inplace=True)
        
        # Drop the merge indicator column and row_id if present
        drop_cols = ['_merge']
        if 'row_id' in merged.columns:
            drop_cols.append('row_id')
        merged = merged.drop(drop_cols, axis=1)
        return merged[['ccy_pair', 'timestamp', 'price', 'new_price', 'reason']]

    def save(self, df, output_path):
        df.to_csv(output_path, index=False)

if __name__ == '__main__':
    base = Path(__file__).resolve().parents[1]
    ccy = base / 'data/rates_ccy_data.csv'
    price = base / 'data/rates_price_data.parq.gzip'
    spot = base / 'data/rates_spot_rate_data.parq.gzip'
    out = base / 'results/price_data.csv'
    converter = RatesPriceConverter(ccy, price, spot)
    converter.load_data()
    start = time.perf_counter()
    result = converter.process()
    end = time.perf_counter()
    print(f'Processing took {end - start:.2f} seconds')
    converter.save(result, out)