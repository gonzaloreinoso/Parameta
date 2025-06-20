import pandas as pd
import numpy as np
from pathlib import Path
import time

class RatesPriceConverter:
    """
    Convert currency prices based on spot rates and conversion factors.
    
    This class implements a solution for currency rate conversion that handles:
    1. Loading currency pair information, price data and spot rates
    2. Identifying prices that need conversion
    3. Finding appropriate spot rates within the specified time window
    4. Applying the conversion formula: new_price = (price / conversion_factor) + spot_mid_rate
    5. Tracking reasons for any conversion failures
    
    The implementation is optimized for performance using vectorized operations
    where possible and efficient pandas merge operations.
    """
    
    def __init__(self, ccy_path, price_path, spot_path):
        """
        Initialize the converter with paths to required data files.
        
        Args:
            ccy_path (str or Path): Path to CSV file with currency pair information
            price_path (str or Path): Path to parquet file with price data
            spot_path (str or Path): Path to parquet file with spot rate data
        """
        self.ccy_path = Path(ccy_path)
        self.price_path = Path(price_path)
        self.spot_path = Path(spot_path)
        self.ccy_df = None
        self.price_df = None
        self.spot_df = None

    def load_data(self):
        """
        Load and prepare all required data from source files.
        
        Loads currency pair information, price data, and spot rates, 
        and converts timestamps to datetime objects for consistent processing.
        """
        self.ccy_df = pd.read_csv(self.ccy_path)
        self.price_df = pd.read_parquet(self.price_path)
        self.spot_df = pd.read_parquet(self.spot_path)
        
        # Convert timestamps once at load time
        for df in [self.price_df, self.spot_df]:
            df['timestamp'] = pd.to_datetime(df['timestamp'])

    def _prepare_merged_data(self):
        """
        Prepare the initial merged DataFrame with price and currency info.
        
        Merges price data with currency pair information and creates a unique
        identifier for each row to handle duplicates correctly.
        
        Returns:
            DataFrame: Merged data with initial conversion settings
        """
        merged = self.price_df.merge(
            self.ccy_df, 
            on='ccy_pair', 
            how='left', 
            indicator=True
        )
        
        # Add row_id for handling duplicates
        merged['row_id'] = (
            merged['ccy_pair'].astype(str) + '|' + 
            merged['timestamp'].astype(str) + '_' + 
            (merged.groupby(['ccy_pair', 'timestamp']).cumcount() + 1).astype(str)
        )
        
        # Initialize result columns
        merged['new_price'] = merged['price']
        merged['reason'] = 'no conversion required'
        
        return merged

    def _get_spot_rates(self, conversion_needed):
        """
        Get appropriate spot rates for prices requiring conversion.
        
        For each price requiring conversion, finds the most recent spot rate
        within the preceding hour.
        
        Args:
            conversion_needed (DataFrame): Subset of data needing conversion
            
        Returns:
            DataFrame: Prices with matched spot rates where available
        """
        # Sort timestamps once
        sorted_conversions = conversion_needed.sort_values('timestamp')
        sorted_spots = self.spot_df.sort_values('timestamp')
        
        # Use merge_asof for efficient lookback
        return pd.merge_asof(
            sorted_conversions,
            sorted_spots,
            by='ccy_pair',
            left_on='timestamp',
            right_on='timestamp',
            direction='backward'
        )

    def _apply_conversions(self, conversion_window):
        """
        Apply conversion calculations to valid rows.
        
        For rows with a valid spot rate in the previous hour, calculates the
        new price using the conversion formula and updates the reason for conversion.
        
        Args:
            conversion_window (DataFrame): Data requiring conversion with matched spot rates
            
        Returns:
            DataFrame: Conversion results with updated new_price and reason columns
        """
        # Calculate time difference for validity check
        is_valid = (
            conversion_window['timestamp'] - 
            conversion_window['timestamp'].dt.floor('h')
        ).dt.total_seconds() <= 3600

        # Apply conversions where valid
        result = conversion_window.copy()
        result.loc[is_valid, 'new_price'] = (
            result.loc[is_valid, 'price'] / 
            result.loc[is_valid, 'conversion_factor']
        ) + result.loc[is_valid, 'spot_mid_rate']
        
        # Set reasons
        result.loc[is_valid, 'reason'] = 'converted'
        result.loc[~is_valid, 'reason'] = 'no spot_mid_rate in previous hour'
        result.loc[~is_valid, 'new_price'] = np.nan
        
        return result

    def process(self):
        """
        Process all price conversions based on the loaded data.
        
        This is the main method that:
        1. Prepares the merged dataset
        2. Identifies prices requiring conversion
        3. Finds appropriate spot rates
        4. Applies conversions where possible
        5. Returns a consolidated result
        
        Returns:
            DataFrame: Complete results with original and converted prices
        """
        # Prepare initial data
        merged = self._prepare_merged_data()
        
        # Find rows that need conversion
        conversion_needed = merged[merged['convert_price'] == True].copy()
        
        if not conversion_needed.empty:
            # Get and apply conversions
            conversion_window = self._get_spot_rates(conversion_needed)
            result_window = self._apply_conversions(conversion_window)
            
            # Update original DataFrame efficiently
            merged.set_index('row_id', inplace=True)
            result_window.set_index('row_id', inplace=True)
            merged.loc[result_window.index, ['new_price', 'reason']] = result_window[['new_price', 'reason']]
            merged.reset_index(drop=True, inplace=True)
        
        # Return only needed columns
        return merged[['ccy_pair', 'timestamp', 'price', 'new_price', 'reason']]

    def save(self, df, output_path):
        """
        Save results to CSV.
        
        Args:
            df (DataFrame): Results DataFrame to save
            output_path (str or Path): Path where the CSV should be saved
        """
        df.to_csv(output_path, index=False)


def main():
    """
    Main execution function for currency rate conversion.
    
    Sets up paths, initializes the converter, processes data,
    and measures performance.
    """
    base = Path(__file__).resolve().parents[1]
    paths = {
        'ccy': base / 'data/rates_ccy_data.csv',
        'price': base / 'data/rates_price_data.parq.gzip',
        'spot': base / 'data/rates_spot_rate_data.parq.gzip',
        'out': base / 'results/price_data.csv'
    }
    
    converter = RatesPriceConverter(paths['ccy'], paths['price'], paths['spot'])
    converter.load_data()
    
    start = time.perf_counter()
    result = converter.process()
    end = time.perf_counter()
    
    print(f'Processing took {end - start:.2f} seconds')
    converter.save(result, paths['out'])


if __name__ == '__main__':
    main()