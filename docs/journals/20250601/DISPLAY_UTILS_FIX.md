# Fix for datetime handling in display_results function

## Issue

The `display_results` function in `utils/for_demo/dsm_display_utils.py` was assuming that 'open_time' would always be available as a column in the DataFrame. However, due to our datetime handling improvements, 'open_time' might sometimes be present as the DataFrame index instead of a column.

This was causing a KeyError when trying to access `df["open_time"]` in examples like `dsm_demo_module.py`.

## Fix

Updated the `display_results` function to check for 'open_time' in both places:

1. First, check if 'open_time' is available as a column:

   ```python
   if "open_time" in df.columns:
       logger.debug("Using open_time from DataFrame columns")
       df["date"] = df["open_time"].dt.date
   ```

2. If not, check if it's available as the index name or if the index is a DatetimeIndex:

   ```python
   elif df.index.name == "open_time" or isinstance(df.index, pd.DatetimeIndex):
       logger.debug("Using open_time from DataFrame index")
       df["date"] = df.index.date
   ```

3. If 'open_time' is not found in either place, log a warning and skip the timeline display.

## Benefits

1. Now works with DataFrames that have 'open_time' either as a column or as the index
2. More robust handling of different DataFrame structures
3. Better error messaging when 'open_time' cannot be found
4. Maintains backward compatibility with existing code

## Testing

- Successfully tested with `dsm_demo_module.py`
- Verified that both the BTCUSDT and ETHUSDT examples work correctly
- Timeline visualization now displays properly in all cases
