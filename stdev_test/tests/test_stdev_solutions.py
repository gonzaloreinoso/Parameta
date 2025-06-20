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

# Import the solution
from scripts.stdev_solution_b import RollingStdevCalculator

class TestStdevSolutions(unittest.TestCase):
    
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
        self.output_path = self.temp_dir / 'test_output.csv'
        
        # Save test data
        self.test_data.to_parquet(self.input_path, compression='gzip')
        
        # Set date range for testing
        self.start_date = '2021-11-20 08:00:00'  # After window size
        self.end_date = '2021-11-22 08:00:00'

    def tearDown(self):
        """Clean up test data"""
        self.test_dir.cleanup()

    def test_solution(self):
        """Test standard deviation calculation"""
        # Initialize calculator
        calculator = RollingStdevCalculator(str(self.input_path))
        calculator.load_data()
        
        # Run calculation
        results = calculator.process(
            start_time=self.start_date, 
            end_time=self.end_date
        )
        
        # Save results
        calculator.save(results, str(self.output_path))
        
        # Check if output file exists
        self.assertTrue(os.path.exists(self.output_path))
        
        # Load results
        results_df = pd.read_csv(self.output_path)
        
        # Basic validation
        self.assertGreater(len(results_df), 0, "Results should not be empty")
        self.assertTrue(all(col in results_df.columns for col in ['security_id', 'timestamp', 'stdev_bid', 'stdev_mid', 'stdev_ask']), 
                       "Output should contain all required columns")
        
        # Check if results only contain timestamps within specified date range
        results_df['timestamp'] = pd.to_datetime(results_df['timestamp'])
        self.assertTrue(
            (results_df['timestamp'] >= pd.to_datetime(self.start_date)).all() and 
            (results_df['timestamp'] <= pd.to_datetime(self.end_date)).all(),
            "Results should only contain timestamps within the specified date range"
        )

if __name__ == '__main__':
    unittest.main()
