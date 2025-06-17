import pandas as pd
import numpy as np
from pathlib import Path

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
        merged['new_price'] = np.nan
        merged['reason'] = ''
        # Pre-sort spot rates for efficient lookup
        self.spot_df = self.spot_df.sort_values(['ccy_pair', 'timestamp'])
        # Group spot rates for fast access
        spot_grouped = self.spot_df.groupby('ccy_pair')
        for idx, row in merged.iterrows():
            ccy_pair = row['ccy_pair']
            ts = row['timestamp']
            if row['_merge'] == 'left_only' or pd.isna(row['convert_price']) or pd.isna(row['conversion_factor']):
                merged.at[idx, 'reason'] = 'ccy_pair not supported or missing conversion info'
                continue
            if not row['convert_price']:
                merged.at[idx, 'new_price'] = row['price']
                merged.at[idx, 'reason'] = 'no conversion required'
                continue
            # Conversion required: find most recent spot_mid_rate in previous hour
            prev_hour = ts.floor('h') - pd.Timedelta(hours=1)
            mask = (spot_grouped.get_group(ccy_pair)['timestamp'] > prev_hour) & (spot_grouped.get_group(ccy_pair)['timestamp'] <= ts)
            spot_candidates = spot_grouped.get_group(ccy_pair)[mask]
            if spot_candidates.empty:
                merged.at[idx, 'reason'] = 'no spot_mid_rate in previous hour'
                continue
            spot_row = spot_candidates.iloc[-1]
            new_price = (row['price'] / row['conversion_factor']) + spot_row['spot_mid_rate']
            merged.at[idx, 'new_price'] = new_price
            merged.at[idx, 'reason'] = 'converted'
        return merged[['ccy_pair', 'timestamp', 'price', 'new_price', 'reason']]

    def save(self, df, output_path):
        df.to_csv(output_path, index=False)

if __name__ == '__main__':
    base = Path(__file__).resolve().parents[1]
    ccy = base / 'data/rates_ccy_data.csv'
    price = base / 'data/rates_price_data.parq.gzip'
    spot = base / 'data/rates_spot_rate_data.parq.gzip'
    out = base / 'results/rates_price_converted.csv'
    converter = RatesPriceConverter(ccy, price, spot)
    converter.load_data()
    result = converter.process()
    converter.save(result, out)
    print(f'Output written to {out}')
