import pandas as pd
from pathlib import Path

class RollingStdevCalculator:
    def __init__(self, price_path):
        """
        Initialize with path to the gzip-compressed parquet file containing columns:
        ['timestamp', 'security_id', 'bid', 'mid', 'ask'].
        """
        self.price_path = Path(price_path)
        self.df = None

    def load_data(self):
        """
        Load the parquet file into a DataFrame, parse timestamps, and sort.
        """
        # Read parquet
        self.df = pd.read_parquet(self.price_path)
        # Ensure timestamp is datetime and sort by timestamp ascending
        self.df['timestamp'] = pd.to_datetime(self.df['snap_time'])
        self.df.sort_values('timestamp', inplace=True)

    def process(self, start_time, end_time):
        """
        Compute rolling standard deviations for each security_id and price type (bid, mid, ask)
        for every snap between start_time and end_time (inclusive).

        For each snap timestamp t, we look back 7 days and find the most recent contiguous
        hourly block of at least 20 snaps for that security. We compute std over the last 20
        within that block, even if they do not end exactly at t.

        Returns a DataFrame with columns ['timestamp', 'security_id', 'stdev_bid', 'stdev_mid', 'stdev_ask']
        and one row per original snap within [start_time, end_time].
        """
        df = self.df.copy()
        # Define time boundaries
        start = pd.to_datetime(start_time)
        end = pd.to_datetime(end_time)

        # Identify breaks in hourly snaps within each security
        df['gap'] = (df.groupby('security_id')['timestamp']
                       .diff().ne(pd.Timedelta(hours=1))).astype(int)
        # Block id increases whenever there's a gap or at group start
        df['block'] = df.groupby('security_id')['gap'].cumsum()

        # Compute rolling std within each block for each price type
        df['stdev_bid'] = (
            df.groupby(['security_id', 'block'])['bid']
              .transform(lambda x: x.rolling(window=20, min_periods=20).std())
        )
        df['stdev_mid'] = (
            df.groupby(['security_id', 'block'])['mid']
              .transform(lambda x: x.rolling(window=20, min_periods=20).std())
        )
        df['stdev_ask'] = (
            df.groupby(['security_id', 'block'])['ask']
              .transform(lambda x: x.rolling(window=20, min_periods=20).std())
        )

        # Prepare a DataFrame of valid stdev values (timestamp when the rolling window completes)
        stdev_df = (
            df.dropna(subset=['stdev_bid', 'stdev_mid', 'stdev_ask'])
              [['security_id', 'timestamp', 'stdev_bid', 'stdev_mid', 'stdev_ask']]
              .drop_duplicates()
              .sort_values('timestamp')
        )

        # Filter original snaps to the requested window
        window_df = (
            df[(df['timestamp'] >= start) & (df['timestamp'] <= end)]
              [['security_id', 'timestamp']]
              .sort_values('timestamp')
        )

        # For each snap, merge the most recent stdev within 7 days before the snap
        result = pd.merge_asof(
            window_df,
            stdev_df,
            by='security_id',
            on='timestamp',
            direction='backward',
            tolerance=pd.Timedelta(days=7)
        )

        return result.sort_values(['security_id', 'timestamp'])

    def save(self, result_df, out_path):
        """
        Save the result DataFrame to CSV at out_path.
        """
        out_path = Path(out_path)
        result_df.to_csv(out_path, index=False)


if __name__ == '__main__':
    import time

    base = Path(__file__).resolve().parents[1]
    price = base / 'data/stdev_price_data.parq.gzip'
    out = base / 'results/stdev_b.csv'

    calc = RollingStdevCalculator(price)
    calc.load_data()

    start = time.perf_counter()
    result = calc.process('2021-11-20 00:00:00', '2021-11-23 09:00:00')
    end = time.perf_counter()

    calc.save(result, out)
    print(f"Processing time: {end - start:.4f} seconds")
    print(f"Output written to {out}")
