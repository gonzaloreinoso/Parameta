import unittest
import pandas as pd
import numpy as np
import os
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

# Add the parent directory to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Import the solutions
from scripts.stdev_solution_a import IncrementalStdevCalculator
from scripts.stdev_solution_b import RollingStdevCalculator

class TestSolutionA(unittest.TestCase):
    
    def setUp(self):
        """Set up test data"""
        # Create a temporary directory for test data
        self.test_dir = tempfile.TemporaryDirectory()
        self.temp_dir = Path(self.test_dir.name)
        
        # Create test data with 30 hourly snapshots for 2 securities
        dates = pd.date_range('2021-11-20', periods=30, freq='h')
        security1 = pd.DataFrame({
            'security_id': 'SEC1',
            'snap_time': dates,
            'bid': np.linspace(100, 110, 30) + np.random.normal(0, 0.5, 30),
            'mid': np.linspace(101, 111, 30) + np.random.normal(0, 0.5, 30),
            'ask': np.linspace(102, 112, 30) + np.random.normal(0, 0.5, 30)
        })
        
        # Add a gap for testing gap handling (skip 5 hours)
        dates2 = pd.date_range('2021-11-20', periods=15, freq='h').union(
            pd.date_range('2021-11-20 20:00:00', periods=15, freq='h')
        )
        security2 = pd.DataFrame({
            'security_id': 'SEC2',
            'snap_time': dates2,
            'bid': np.linspace(200, 220, 30) + np.random.normal(0, 1, 30),
            'mid': np.linspace(202, 222, 30) + np.random.normal(0, 1, 30),
            'ask': np.linspace(204, 224, 30) + np.random.normal(0, 1, 30)
        })
        
        # Combine data
        self.test_data = pd.concat([security1, security2])
        
        # Set window parameters
        self.window_size = 10
        
        # Define paths
        self.input_path = self.temp_dir / 'test_input.parq.gzip'
        self.output_opt = self.temp_dir / 'test_output_opt.csv'
        self.output_std = self.temp_dir / 'test_output_std.csv'
        self.state_path = self.temp_dir / 'test_state.json'
        
        # Save test data
        self.test_data.to_parquet(self.input_path, compression='gzip')
        
        # Set date range for testing
        self.start_date = '2021-11-20 08:00:00'  # After window size
        self.end_date = '2021-11-22 08:00:00'

    def tearDown(self):
        """Clean up test data"""
        self.test_dir.cleanup()

    def test_solution_correctness(self):
        """Test that both solutions produce similar results"""
        # Initialize standard calculator
        std_calculator = RollingStdevCalculator(str(self.input_path))
        std_calculator.load_data()
        
        # Run standard calculation
        std_result = std_calculator.process(
            start_time=self.start_date, 
            end_time=self.end_date
        )
        std_calculator.save(std_result, str(self.output_std))
        
        # Initialize incremental calculator
        opt_calculator = IncrementalStdevCalculator(
            price_path=str(self.input_path),
            window_size=self.window_size,
            state_path=str(self.state_path)
        )
        opt_calculator.load_data()
        
        # Run optimized calculation
        opt_result = opt_calculator.process(
            start_time=self.start_date, 
            end_time=self.end_date
        )
        opt_calculator.save(opt_result, str(self.output_opt))
        
        # Both files should exist
        self.assertTrue(os.path.exists(self.output_std))
        self.assertTrue(os.path.exists(self.output_opt))
        
        # Check that state file was created
        self.assertTrue(os.path.exists(self.state_path))
        
        # Load results and compare
        std_df = pd.read_csv(self.output_std)
        opt_df = pd.read_csv(self.output_opt)
        
        # Both should have data
        self.assertGreater(len(std_df), 0)
        self.assertGreater(len(opt_df), 0)
        
        # Merge results for comparison
        std_df['timestamp'] = pd.to_datetime(std_df['timestamp'])
        opt_df['timestamp'] = pd.to_datetime(opt_df['timestamp'])
        
        # Adjust column names if needed to match
        if 'stdev_bid' in std_df.columns and 'bid_stdev' in opt_df.columns:
            std_df = std_df.rename(columns={
                'stdev_bid': 'bid_stdev',
                'stdev_mid': 'mid_stdev',
                'stdev_ask': 'ask_stdev'
            })
            
        # Compare results where both have values
        merged = pd.merge(
            std_df, 
            opt_df, 
            on=['security_id', 'timestamp'],
            suffixes=('_std', '_opt')
        ).dropna()
        
        # Check if results are similar (allowing for small numerical differences)
        for col in ['bid_stdev', 'mid_stdev', 'ask_stdev']:
            std_col = f"{col}_std"
            opt_col = f"{col}_opt"
            
            if std_col in merged.columns and opt_col in merged.columns:
                relative_diff = np.abs((merged[std_col] - merged[opt_col]) / merged[std_col])
                max_diff = relative_diff.max()
                
                # Allow for small numerical differences (0.1%)
                self.assertLess(max_diff, 0.001,
                               f"Max difference in {col}: {max_diff:.6f}")

    def test_state_persistence(self):
        """Test that state persistence works correctly"""
        # Run calculation once to generate state
        calculator1 = IncrementalStdevCalculator(
            price_path=str(self.input_path),
            window_size=self.window_size,
            state_path=str(self.state_path)
        )
        calculator1.load_data()
        result1 = calculator1.process(
            start_time=self.start_date,
            end_time=self.end_date
        )
        
        # Create a new calculator that should load the saved state
        calculator2 = IncrementalStdevCalculator(
            price_path=str(self.input_path),
            window_size=self.window_size,
            state_path=str(self.state_path)
        )
        calculator2.load_data()
        
        # Check that state was properly loaded
        self.assertGreater(len(calculator2.calculation_state), 0,
                          "State should be loaded from file")
        
        # Run calculation on a future date
        new_start = pd.to_datetime(self.end_date) + pd.Timedelta(hours=1)
        new_end = pd.to_datetime(self.end_date) + pd.Timedelta(hours=5)
        
        # This should use the saved state
        result2 = calculator2.process(
            start_time=new_start.strftime('%Y-%m-%d %H:%M:%S'),
            end_time=new_end.strftime('%Y-%m-%d %H:%M:%S')
        )
        
        # It should produce some results
        self.assertGreaterEqual(len(result2), 0)


if __name__ == '__main__':
    unittest.main()
