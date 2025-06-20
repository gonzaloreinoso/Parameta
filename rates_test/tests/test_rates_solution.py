import unittest
import pandas as pd
import numpy as np
import os
import tempfile
from pathlib import Path
import sys

# Add the parent directory to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Import the solution
from scripts.rates_solution import RatesPriceConverter

class TestRatesPriceConverter(unittest.TestCase):
    
    def setUp(self):
        """Set up test data"""
        # Create a temporary directory for test data
        self.test_dir = tempfile.TemporaryDirectory()
        self.temp_dir = Path(self.test_dir.name)
        
        # Create test currency data
        ccy_data = pd.DataFrame({
            'ccy_pair': ['EUR/USD', 'GBP/USD', 'JPY/USD', 'AUD/USD'],
            'convert_price': [True, True, False, True],
            'conversion_factor': [1.2, 1.5, 110.0, 0.75]
        })
        
        # Create test price data
        dates = pd.date_range('2021-01-01', periods=10, freq='h')
        price_data = []
        for ccy in ccy_data['ccy_pair']:
            for date in dates:
                price_data.append({
                    'ccy_pair': ccy,
                    'timestamp': date,
                    'price': np.random.uniform(0.5, 2.0)
                })
        price_df = pd.DataFrame(price_data)
        
        # Create spot rate data (with some rates missing to test edge cases)
        spot_data = []
        for ccy in ccy_data['ccy_pair']:
            if ccy != 'JPY/USD':  # Skip one to test missing spot rates
                for date in dates[:-1]:  # Make one missing at the end
                    spot_data.append({
                        'ccy_pair': ccy,
                        'timestamp': date - pd.Timedelta(minutes=30),  # 30 min before price
                        'spot_mid_rate': np.random.uniform(0.8, 1.2)
                    })
        spot_df = pd.DataFrame(spot_data)
        
        # Save test data
        self.ccy_path = self.temp_dir / 'test_ccy_data.csv'
        self.price_path = self.temp_dir / 'test_price_data.parq.gzip'
        self.spot_path = self.temp_dir / 'test_spot_rate_data.parq.gzip'
        self.output_path = self.temp_dir / 'test_output.csv'
        
        ccy_data.to_csv(self.ccy_path, index=False)
        price_df.to_parquet(self.price_path, compression='gzip')
        spot_df.to_parquet(self.spot_path, compression='gzip')

    def tearDown(self):
        """Clean up test data"""
        self.test_dir.cleanup()

    def test_conversion(self):
        """Test the price conversion process"""
        # Initialize converter
        converter = RatesPriceConverter(
            str(self.ccy_path), 
            str(self.price_path), 
            str(self.spot_path)
        )
        converter.load_data()
        
        # Run conversion
        result = converter.process()
        converter.save(result, str(self.output_path))
        
        # Check if output file exists
        self.assertTrue(os.path.exists(self.output_path))
        
        # Load results
        results_df = pd.read_csv(self.output_path)
        
        # Basic validation
        self.assertGreater(len(results_df), 0, "Results should not be empty")
        self.assertTrue(all(col in results_df.columns for col in 
                           ['ccy_pair', 'timestamp', 'price', 'new_price', 'reason']), 
                       "Output should contain all required columns")
        
        # Check conversion logic for different cases
        no_conversion = results_df[results_df['ccy_pair'] == 'JPY/USD']
        self.assertTrue(all(no_conversion['reason'] == 'no conversion required'),
                       "JPY/USD should not require conversion")
        
        converted = results_df[(results_df['ccy_pair'] == 'EUR/USD') & 
                              (results_df['reason'] == 'converted')]
        
        # Verify at least some conversions happened
        self.assertGreater(len(converted), 0, "Some EUR/USD prices should be converted")
        

if __name__ == '__main__':
    unittest.main()
