import pandas as pd
import numpy as np
from pathlib import Path

class RollingStdevCalculator:
    def __init__(self, price_path):
        self.price_path = Path(price_path)
        self.df = None

    def load_data(self):
        self.df = pd.read_parquet(self.price_path)
        self.df['timestamp'] = pd.to_datetime(self.df['snap_time'])

    def process(self, start, end, window=20):
        # Filter for the relevant snap range
        mask = (self.df['snap_time'] >= pd.to_datetime(start)) & (self.df['snap_time'] <= pd.to_datetime(end))
        snap_times = pd.date_range(start=start, end=end, freq='h')
        results = []
        for sec_id, group in self.df.groupby('security_id'):
            group = group.sort_values('snap_time')
            group = group.set_index('snap_time')
            for snap in snap_times:
                # Look back 7 days, get up to 20 contiguous hourly snaps
                lookback_start = snap - pd.Timedelta(days=7)
                window_data = group.loc[(group.index > lookback_start) & (group.index <= snap)]
                # Ensure contiguous: last 20 hourly snaps with no gaps
                if len(window_data) < window:
                    continue
                # Check for contiguous hours
                last_hours = window_data.tail(window).index
                if not all((last_hours[1:] - last_hours[:-1]) == pd.Timedelta(hours=1)):
                    continue
                row = {'security_id': sec_id, 'snap_time': snap}
                for col in ['bid', 'mid', 'ask']:
                    row[f'{col}_stdev'] = window_data[col].tail(window).std(ddof=0)
                results.append(row)
        return pd.DataFrame(results)

    def save(self, df, output_path):
        df.to_csv(output_path, index=False)

if __name__ == '__main__':
    base = Path(__file__).resolve().parents[1]
    price = base / 'data/stdev_price_data.parq.gzip'
    out = base / 'results/stdev_rolling.csv'
    calc = RollingStdevCalculator(price)
    calc.load_data()
    result = calc.process('2021-11-20 00:00:00', '2021-11-23 09:00:00')
    calc.save(result, out)
    print(f'Output written to {out}')
