# Parameta Financial Data Analysis

This repository contains tools for financial data analysis, specifically focused on calculating rolling standard deviations and currency rate conversions.

## Repository Structure

```
Parameta/
├── requirements.txt       # Python dependencies
├── rates_test/           # Currency rate conversion tools
│   ├── data/             # Input data files
│   ├── results/          # Output files
│   └── scripts/          # Python scripts
└── stdev_test/           # Standard deviation calculation tools
    ├── data/             # Input data files
    ├── results/          # Output files
    └── scripts/          # Python scripts
```

## Requirements

- Python 3.8+
- Dependencies listed in requirements.txt:
  - pandas >= 1.3.0
  - numpy >= 1.21.0
  - pyarrow >= 5.0.0
  - psutil >= 5.9.0

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

## Usage

### Standard Deviation Calculation

The stdev_test directory contains two implementations for calculating rolling standard deviations:

#### Solution A - Incremental Approach:

```bash
cd stdev_test/scripts
python stdev_solution_a.py
```

This implementation uses a custom window-based approach with incremental calculation.

#### Solution B - Recalculation Approach:

```bash
cd stdev_test/scripts
python stdev_solution_b.py
```

This implementation uses pandas' built-in vectorized operations for faster processing.

### Currency Rate Conversion

The rates_test directory contains tools for currency rate conversion:

```bash
cd rates_test/scripts
python rates_solution.py
```

## Data Files

### stdev_test/data
- `stdev_price_data.parq.gzip`: Contains time series of bid, mid, and ask prices per security ID

### rates_test/data
- `rates_ccy_data.csv`: Contains currency pair information
- `rates_price_data.parq.gzip`: Contains price data for currency pairs
- `rates_spot_rate_data.parq.gzip`: Contains spot rate data for currency conversion

## Results

When you run the scripts, the results will be stored in the respective results directories:

## Results

The output files are stored in the corresponding results directories:

- `stdev_test/results/stdev_a.csv`: Results from the state standard deviation calculation
- `stdev_test/results/stdev_b.csv`: Results from the reevaluation standard deviation calculation
- `rates_test/results/price_data.csv`: Results from currency rate conversion
