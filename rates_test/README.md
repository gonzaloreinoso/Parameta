# Currency Rate Converter

This module contains tools for converting currency prices based on spot rates and conversion factors.

## Problem Statement

Convert currency prices based on:
- Currency pair information (from rates_ccy_data.csv)
- Spot rates (from rates_spot_rate_data.parq.gzip)
- Only use spot rates within the hour prior to the price timestamp

## Implementation

The solution (`rates_solution.py`) handles the following:
- Merges price data with currency pair information
- Finds the most recent valid spot rate for each price point
- Applies conversions based on the formula: `new_price = (price / conversion_factor) + spot_mid_rate`
- Validates time constraints (spot rate must be within previous hour)
- Tracks reasons for any failed conversions

## Data Files

### Input Files

1. `rates_ccy_data.csv` - Currency pair information:
   - `ccy_pair` - Currency pair identifier
   - `convert_price` - Boolean flag indicating if conversion is needed
   - `conversion_factor` - Factor to apply for conversion

2. `rates_price_data.parq.gzip` - Price data:
   - `ccy_pair` - Currency pair identifier
   - `timestamp` - Time of the price snapshot
   - `price` - Price value

3. `rates_spot_rate_data.parq.gzip` - Spot rate data:
   - `ccy_pair` - Currency pair identifier
   - `timestamp` - Time of the spot rate
   - `spot_mid_rate` - Mid-point spot rate

### Output File

`price_data.csv` - Converted price data:
- `ccy_pair` - Currency pair identifier
- `timestamp` - Time of the price snapshot
- `price` - Original price value
- `new_price` - Converted price value
- `reason` - Reason for conversion outcome

## Usage

```bash
# Run the rate converter
python scripts/rates_solution.py
```
