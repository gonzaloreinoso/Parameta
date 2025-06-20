import pandas as pd
import numpy as np
from pathlib import Path
import time
import json


class IncrementalStdevCalculator:
    """
    Calculate rolling standard deviations incrementally by maintaining state data.
    
    This implementation maintains running sums and squared sums for each security ID
    and price type (bid, mid, ask), allowing for efficient updates as new data points
    arrive. When a time gap is detected, the calculation state is reset.
    """
    
    def __init__(self, price_path, window_size=20, state_path=None):
        """
        Initialize with path to the data file and calculation parameters.
        
        Args:
            price_path (str): Path to the gzip-compressed parquet file
            window_size (int): Size of the rolling window for standard deviation
            state_path (str, optional): Path to store/load calculation state
        """
        self.price_path = Path(price_path)
        self.window_size = window_size
        self.state_path = Path(state_path) if state_path else None
        self.df = None
        self.calculation_state = {}
        
    def _initialize_state(self):
        """Initialize or reset the calculation state dictionary."""
        self.calculation_state = {}
        
    def _get_state_key(self, security_id, price_type):
        """Generate a unique key for storing state data."""
        return f"{security_id}_{price_type}"
        
    def _reset_security_state(self, security_id):
        """Reset calculation state for a specific security ID."""
        for price_type in ['bid', 'mid', 'ask']:
            key = self._get_state_key(security_id, price_type)
            if key in self.calculation_state:
                del self.calculation_state[key]
                
    def load_data(self):
        """Load the parquet file into a DataFrame, parse timestamps, and sort."""
        # Read parquet
        self.df = pd.read_parquet(self.price_path)
        # Ensure timestamp is datetime and sort by timestamp ascending
        self.df['timestamp'] = pd.to_datetime(self.df['snap_time'])
        self.df.sort_values(['security_id', 'timestamp'], inplace=True)
          # Try to load saved state if available
        if self.state_path and self.state_path.exists():
            try:
                with open(self.state_path, 'r') as f:
                    loaded_state = json.load(f)
                    
                    # Convert string timestamps back to datetime objects
                    self.calculation_state = {}
                    for key, state in loaded_state.items():
                        self.calculation_state[key] = {
                            'values': state['values'],
                            'timestamps': [pd.Timestamp(ts) for ts in state['timestamps']],
                            'sum': state['sum'],
                            'sum_sq': state['sum_sq'],
                            'last_timestamp': pd.Timestamp(state['last_timestamp']) if state['last_timestamp'] else None
                        }
            except (json.JSONDecodeError, IOError):
                self._initialize_state()
        else:
            self._initialize_state()
            
    def _update_state(self, security_id, price_type, values, timestamps):
        """
        Update calculation state for a given security and price type.
        
        Args:
            security_id (str): The security identifier
            price_type (str): The price type ('bid', 'mid', or 'ask')
            values (array): Array of price values
            timestamps (array): Array of timestamps matching the values
            
        Returns:
            list: Standard deviations for each point where window is complete
        """
        key = self._get_state_key(security_id, price_type)
        stdevs = []
        
        # Get or initialize state for this security/price type
        if key not in self.calculation_state:
            self.calculation_state[key] = {
                'values': [],
                'timestamps': [],
                'sum': 0.0,
                'sum_sq': 0.0,
                'last_timestamp': None
            }
            
        state = self.calculation_state[key]
        
        for value, ts in zip(values, timestamps):
            # Check for time gap - reset if not contiguous hourly data
            if state['last_timestamp'] is not None:
                expected_ts = state['last_timestamp'] + pd.Timedelta(hours=1)
                if ts != expected_ts:
                    # Reset state on gap
                    state['values'] = []
                    state['timestamps'] = []
                    state['sum'] = 0.0
                    state['sum_sq'] = 0.0
            
            # Update running sums
            state['values'].append(value)
            state['timestamps'].append(ts)
            state['sum'] += value
            state['sum_sq'] += value * value
            state['last_timestamp'] = ts
            
            # Remove oldest value if window is full
            if len(state['values']) > self.window_size:
                old_value = state['values'].pop(0)
                state['timestamps'].pop(0)
                state['sum'] -= old_value
                state['sum_sq'] -= old_value * old_value
            
            # Calculate stdev if we have a full window
            if len(state['values']) == self.window_size:
                variance = (state['sum_sq'] / self.window_size) - (state['sum'] / self.window_size)**2
                # Correct for numerical issues that can cause slightly negative variance
                variance = max(0, variance)
                stdevs.append(np.sqrt(variance))
            else:
                stdevs.append(None)
                
        return stdevs
        
    def process(self, start_time, end_time):
        """
        Process data and calculate standard deviations for the specified time range.
        
        Args:
            start_time (str): Start time in format 'YYYY-MM-DD HH:MM:SS'
            end_time (str): End time in format 'YYYY-MM-DD HH:MM:SS'
            
        Returns:
            DataFrame: Results with calculated standard deviations
        """
        start = pd.to_datetime(start_time)
        end = pd.to_datetime(end_time)
        
        # Filter data to include 7 days before start for window calculation
        lookback_start = start - pd.Timedelta(days=7)
        processing_df = self.df[self.df['timestamp'] >= lookback_start].copy()
        
        # Initialize result structure
        results = []
        
        # Process each security ID
        for security_id, group in processing_df.groupby('security_id'):
            group = group.sort_values('timestamp')
            
            # Update state and calculate stdevs for each price type
            bid_stdevs = self._update_state(security_id, 'bid', group['bid'].values, group['timestamp'].values)
            mid_stdevs = self._update_state(security_id, 'mid', group['mid'].values, group['timestamp'].values)
            ask_stdevs = self._update_state(security_id, 'ask', group['ask'].values, group['timestamp'].values)
            
            # Compile results for this security
            for i, (ts, bid_stdev, mid_stdev, ask_stdev) in enumerate(zip(
                group['timestamp'].values, bid_stdevs, mid_stdevs, ask_stdevs)):
                
                # Only include results in the requested time range
                if ts >= start and ts <= end:
                    results.append({
                        'security_id': security_id,
                        'timestamp': ts,
                        'bid_stdev': bid_stdev,
                        'mid_stdev': mid_stdev,
                        'ask_stdev': ask_stdev
                    })
        
        # Convert to DataFrame and handle missing values
        result_df = pd.DataFrame(results)
          # Save current state
        if self.state_path:
            # Convert timestamps to strings for JSON serialization
            serializable_state = {}
            for key, state in self.calculation_state.items():
                serializable_state[key] = {
                    'values': state['values'],
                    'timestamps': [pd.Timestamp(ts).isoformat() for ts in state['timestamps']],
                    'sum': state['sum'],
                    'sum_sq': state['sum_sq'],
                    'last_timestamp': pd.Timestamp(state['last_timestamp']).isoformat() if state['last_timestamp'] else None
                }
            
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.state_path, 'w') as f:
                json.dump(serializable_state, f)
        
        return result_df.sort_values(['security_id', 'timestamp'])
    
    def save(self, result_df, out_path):
        """
        Save the result DataFrame to CSV.
        
        Args:
            result_df (DataFrame): The results to save
            out_path (str): Path where the CSV should be saved
        """
        out_path = Path(out_path)
        result_df.to_csv(out_path, index=False)


if __name__ == '__main__':
    # Time execution
    start_time = time.perf_counter()
    
    # Set up paths
    base = Path(__file__).resolve().parents[1]
    price_path = base / 'data/stdev_price_data.parq.gzip'
    out_path = base / 'results/stdev_optimized.csv'
    state_path = base / 'results/calculation_state.json'
    
    # Initialize calculator with state persistence
    calculator = IncrementalStdevCalculator(
        price_path=price_path, 
        window_size=20,
        state_path=state_path
    )
    
    # Load data and process
    calculator.load_data()
    result = calculator.process('2021-11-20 00:00:00', '2021-11-23 09:00:00')
    
    # Save results
    calculator.save(result, out_path)
    
    # Report execution time
    end_time = time.perf_counter()
    print(f"Processing time: {end_time - start_time:.4f} seconds")
    print(f"Output written to {out_path}")
    print(f"Calculation state saved to {state_path}")
