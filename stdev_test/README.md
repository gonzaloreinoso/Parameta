# Standard Deviation Calculator

This module contains two implementations for calculating rolling standard deviations of financial price data.

## Problem Statement

Calculate rolling standard deviations for bid, mid, and ask prices per security ID over a specified time window. The calculations must handle:
- Hourly data snapshots
- Contiguous time periods (reset calculation after gaps)
- 20-hour rolling window by default

## Implementations

### Solution A (`stdev_solution_a.py`)
- Uses a custom window-based approach with incremental calculation
- Maintains running sums for more efficient standard deviation calculation
- Resets windows on time gaps

### Solution B (`stdev_solution_b.py`)
- Uses pandas' built-in vectorized operations for faster processing
- Identifies contiguous time sequences for proper window resets
- More concise code with potentially better performance for large datasets

## Data Format

Input file `stdev_price_data.parq.gzip` is a Parquet file with the following structure:

| Column       | Type      | Description                    |
|------------- |-----------|--------------------------------|
| security_id  | string    | Unique identifier for security |
| snap_time    | timestamp | Time of the price snapshot     |
| bid          | float     | Bid price                     |
| mid          | float     | Mid price                     |
| ask          | float     | Ask price                     |

## Output Format

Both solutions produce CSV files with the following structure:

| Column       | Type      | Description                        |
|-------------|-----------|------------------------------------|
| security_id  | string    | Unique identifier for security     |
| snap_time    | timestamp | Time of the price snapshot         |
| bid_stdev    | float     | Standard deviation of bid prices   |
| mid_stdev    | float     | Standard deviation of mid prices   |
| ask_stdev    | float     | Standard deviation of ask prices   |

## Usage

```bash
# Run solution A
python scripts/stdev_solution_a.py

# Run solution B
python scripts/stdev_solution_b.py
```
