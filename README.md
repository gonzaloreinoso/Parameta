# Parameta Financial Data Analysis

This repository contains efficient, class-based implementations for solving two financial data analysis problems:
1. Currency rate conversions with proper spot rate calculations
2. Rolling standard deviation calculations with contiguous time window handling

## Repository Structure

```
Parameta/
├── requirements.txt       # Python dependencies
├── rates_test/           # Currency rate conversion solution (Problem 1)
│   ├── data/             # Input data files (.csv and .parq.gzip)
│   ├── results/          # Output files (.csv)
│   └── scripts/          # Python implementation
└── stdev_test/           # Standard deviation solution (Problem 2)
    ├── data/             # Input data files (.parq.gzip)
    ├── results/          # Output files (.csv)
    ├── scripts/          # Python implementation
    └── tests/            # Unit tests
```

## Requirements

- Python 3.8+
- Dependencies listed in requirements.txt:
  - pandas >= 1.3.0
  - numpy >= 1.21.0
  - pyarrow >= 5.0.0
  - psutil >= 5.9.0 (for memory monitoring)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/Parameta.git
cd Parameta
```

2. Create a virtual environment (recommended):
```bash
python -m venv venv
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Performance

Both solutions are optimized for performance, with execution times under 1 second on a system with 16GB RAM running Python 3, meeting the benchmark requirements specified. This is achieved through:

1. Efficient data loading and preparation
2. Vectorized operations whenever possible
3. Optimized pandas operations (using merge_asof, transformations)
4. Proper indexing and grouping strategies

## Usage

### Problem 1: Currency Rate Conversion

This implementation efficiently handles the conversion of currency rates using the correct spot rates within specified time windows:

```bash
cd rates_test/scripts
python rates_solution.py
```

Key features:
- OOP design with clear class structure
- Efficient pandas/numpy vectorized operations
- Proper handling of spot rate lookups within time windows
- Performance optimized (< 1 second processing time)

### Problem 2: Standard Deviation Calculation

Two implementations are provided for calculating rolling standard deviations:

#### Solution A (Incremental Approach)

```bash
cd stdev_test/scripts
python stdev_solution_a.py
```

Key features:
- OOP design with state management
- Maintains running sums for incremental updates
- Stores calculation state for efficient processing
- Ideal for continuous processing scenarios
- Performance optimized (< 1 second processing time)

#### Solution B (Vectorized Approach)

```bash
cd stdev_test/scripts
python stdev_solution_b.py
```

Key features:
- OOP design with clean class structure
- Uses pandas' built-in vectorized operations
- Properly identifies contiguous hourly sequences
- Recalculates from scratch for each time point
- Performance optimized (< 1 second processing time)
cd stdev_test/scripts
python stdev_solution_a.py
```

Key features:
- Maintains calculation state between runs
- Uses running sums for efficient updates
- Properly resets calculations on time gaps
- Saves state to JSON for persistence
- Perfect for continuous/real-time processing

## Data Files

### stdev_test/data
- `stdev_price_data.parq.gzip`: Contains time series of bid, mid, and ask prices per security ID

### rates_test/data
- `rates_ccy_data.csv`: Contains currency pair information
- `rates_price_data.parq.gzip`: Contains price data for currency pairs
- `rates_spot_rate_data.parq.gzip`: Contains spot rate data for currency conversion

## Results

When you run the scripts, the results will be stored in the respective results directories:

## Data Files

### Problem 1: Currency Rate Conversion
- `rates_test/data/rates_ccy_data.csv`: Currency pair reference file with conversion information
- `rates_test/data/rates_price_data.parq.gzip`: Timestamped prices for currency pairs
- `rates_test/data/rates_spot_rate_data.parq.gzip`: Timestamped FX rates for currency pairs

### Problem 2: Standard Deviation Calculation
- `stdev_test/data/stdev_price_data.parq.gzip`: Timestamped bid, mid, and ask prices per security ID

## Results

The output files are stored in the corresponding results directories:
- `rates_test/results/price_data.csv`: Results from the currency rate conversion
- `stdev_test/results/stdev_b.csv`: Results from the standard deviation calculation (Solution B)
- `stdev_test/results/stdev_optimized.csv`: Results from the optimized standard deviation calculation
- `stdev_test/results/calculation_state.json`: Saved calculation state for the optimized solution

## Running The Tests

The repository includes unit tests for all solutions:

```bash
# Test the standard deviation solution (Solution B)
cd stdev_test
python -m unittest tests/test_stdev_solutions.py

# Test the optimized standard deviation solution
cd stdev_test
python -m unittest tests/test_stdev_optimized.py

# Test the currency rate conversion solution
cd rates_test
python -m unittest tests/test_rates_solution.py
```

- `stdev_test/results/stdev_a.csv`: Results from the state standard deviation calculation
- `stdev_test/results/stdev_b.csv`: Results from the reevaluation standard deviation calculation
- `rates_test/results/price_data.csv`: Results from currency rate conversion
