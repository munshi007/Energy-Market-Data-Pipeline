"""
Utility functions for energy market data processing

Comprehensive helper functions for energy trading data analysis,
market clearing simulation, and renewable energy impact assessment.

Author: Rohan Munshi
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import logging
import os
import json
from pathlib import Path

logger = logging.getLogger(__name__)

def load_energy_data(file_path: str) -> pd.DataFrame:
    """
    Load energy data from CSV file with basic error handling
    
    Args:
        file_path: Path to the CSV file
        
    Returns:
        DataFrame with loaded energy data
        
    Raises:
        FileNotFoundError: If file doesn't exist
        pd.errors.EmptyDataError: If file is empty
    """
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Loaded {len(df)} records from {file_path}")
        return df
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except pd.errors.EmptyDataError:
        logger.error(f"File is empty: {file_path}")
        raise
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise

def validate_data_schema(df: pd.DataFrame, required_columns: List[str]) -> Dict:
    """
    Validate that DataFrame has required columns and reasonable data types
    
    Args:
        df: DataFrame to validate
        required_columns: List of column names that must be present
        
    Returns:
        Dictionary with validation results
    """
    validation_results = {
        'schema_valid': True,
        'missing_columns': [],
        'issues': []
    }
    
    # Check required columns
    missing_cols = set(required_columns) - set(df.columns)
    if missing_cols:
        validation_results['missing_columns'] = list(missing_cols)
        validation_results['schema_valid'] = False
        validation_results['issues'].append(f"Missing columns: {missing_cols}")
    
    # Check for completely empty columns
    empty_columns = []
    for col in df.columns:
        if df[col].isnull().all():
            empty_columns.append(col)
            validation_results['schema_valid'] = False
    
    if empty_columns:
        validation_results['issues'].append(f"Completely empty columns: {empty_columns}")
    
    # Check data types
    expected_types = {
        'Timestamp': 'object',  # Will be string initially, converted to datetime later
        'Price': ['float64', 'int64'],  # Numerical
        'Volume': ['float64', 'int64'],  # Numerical  
        'Sell_Buy': 'object'  # Categorical string
    }
    
    type_issues = []
    for col, expected_type in expected_types.items():
        if col in df.columns:
            actual_type = str(df[col].dtype)
            if isinstance(expected_type, list):
                if actual_type not in expected_type:
                    type_issues.append(f"Column '{col}' has type '{actual_type}', expected one of {expected_type}")
            else:
                if actual_type != expected_type:
                    type_issues.append(f"Column '{col}' has type '{actual_type}', expected '{expected_type}'")
    
    if type_issues:
        validation_results['issues'].extend(type_issues)
        # Don't fail for type issues, just warn
        logger.warning(f"Data type issues (non-critical): {type_issues}")
    
    # Validate timestamp format (basic check)
    if 'Timestamp' in df.columns:
        try:
            # Try to parse a few timestamps to check format
            sample_timestamps = df['Timestamp'].dropna().head(10)
            pd.to_datetime(sample_timestamps)
            logger.info("Timestamp format validation passed")
        except Exception as e:
            validation_results['issues'].append(f"Timestamp format validation failed: {str(e)}")
            validation_results['schema_valid'] = False
    
    # Check for reasonable data ranges
    if 'Price' in df.columns:
        price_stats = df['Price'].describe()
        if price_stats['min'] < 0:
            validation_results['issues'].append("Found negative prices")
        if price_stats['max'] > 10000:  # Very high price threshold
            validation_results['issues'].append(f"Found extremely high prices (max: {price_stats['max']})")
    
    if 'Volume' in df.columns:
        volume_stats = df['Volume'].describe()
        if volume_stats['min'] < 0:
            validation_results['issues'].append("Found negative volumes")
    
    return validation_results

def generate_data_quality_report(df: pd.DataFrame) -> Dict:
    """
    Generate comprehensive data quality report
    
    Args:
        df: DataFrame to analyze
        
    Returns:
        Dictionary with quality metrics
    """
    report = {
        'total_records': len(df),
        'total_columns': len(df.columns),
        'missing_values': df.isnull().sum().to_dict(),
        'missing_percentages': (df.isnull().sum() / len(df) * 100).round(2).to_dict(),
        'duplicate_rows': df.duplicated().sum(),
        'memory_usage_mb': round(df.memory_usage(deep=True).sum() / 1024**2, 2),
    }
    
    # Outlier detection for numerical columns
    numerical_cols = df.select_dtypes(include=[np.number]).columns
    outliers = {}
    
    for col in numerical_cols:
        if col in df.columns and not df[col].isnull().all():
            # IQR method for outlier detection
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            
            # Define outliers as values beyond 1.5 * IQR from Q1/Q3
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            outlier_mask = (df[col] < lower_bound) | (df[col] > upper_bound)
            outlier_count = outlier_mask.sum()
            
            outliers[col] = {
                'count': int(outlier_count),
                'percentage': round(outlier_count / len(df) * 100, 2),
                'lower_bound': round(lower_bound, 2),
                'upper_bound': round(upper_bound, 2)
            }
    
    report['outliers'] = outliers
    
    # Consistency checks for categorical columns
    categorical_issues = {}
    
    # Check Sell_Buy column specifically
    if 'Sell_Buy' in df.columns:
        sell_buy_values = df['Sell_Buy'].value_counts()
        # Normalize to check for case inconsistencies
        normalized_values = df['Sell_Buy'].str.lower().str.strip().value_counts()
        
        categorical_issues['Sell_Buy'] = {
            'unique_values': sell_buy_values.to_dict(),
            'normalized_values': normalized_values.to_dict(),
            'has_case_inconsistency': len(sell_buy_values) > len(normalized_values)
        }
    
    report['categorical_issues'] = categorical_issues
    
    # Business logic validation
    business_issues = []
    
    # Check for negative prices (shouldn't happen in energy markets)
    if 'Price' in df.columns:
        negative_prices = (df['Price'] < 0).sum()
        if negative_prices > 0:
            business_issues.append(f"Found {negative_prices} negative prices")
    
    # Check for negative volumes (shouldn't happen)
    if 'Volume' in df.columns:
        negative_volumes = (df['Volume'] < 0).sum()
        if negative_volumes > 0:
            business_issues.append(f"Found {negative_volumes} negative volumes")
    
    # Check for extremely high prices (potential data errors)
    if 'Price' in df.columns:
        extremely_high_prices = (df['Price'] > 1000).sum()  # €1000/MWh is very high
        if extremely_high_prices > 0:
            business_issues.append(f"Found {extremely_high_prices} extremely high prices (>€1000/MWh)")
    
    report['business_logic_issues'] = business_issues
    
    return report

def standardize_sell_buy_column(df: pd.DataFrame, column_name: str = 'Sell_Buy') -> pd.DataFrame:
    """
    Standardize the Sell_Buy column to consistent lowercase format
    
    Args:
        df: DataFrame with sell/buy data
        column_name: Name of the column to standardize
        
    Returns:
        DataFrame with standardized column
    """
    df_clean = df.copy()
    
    if column_name not in df_clean.columns:
        logger.warning(f"Column '{column_name}' not found in DataFrame")
        return df_clean
    
    # Store original values for logging
    original_values = df_clean[column_name].value_counts()
    logger.info(f"Original {column_name} values: {original_values.to_dict()}")
    
    # Standardize to lowercase and strip whitespace
    df_clean[column_name] = df_clean[column_name].astype(str).str.lower().str.strip()
    
    # Map any variations to standard values
    standardization_map = {
        'sell': 'sell',
        'buy': 'buy',
        'sale': 'sell',  # Handle potential variations
        'purchase': 'buy'
    }
    
    # Apply standardization
    df_clean[column_name] = df_clean[column_name].map(standardization_map)
    
    # Check for any unmapped values (potential data quality issues)
    unmapped_mask = df_clean[column_name].isnull()
    if unmapped_mask.any():
        unmapped_values = df[unmapped_mask][column_name].value_counts()
        logger.warning(f"Found unmapped values in {column_name}: {unmapped_values.to_dict()}")
        
        # For unmapped values, try to infer from first letter
        original_unmapped = df.loc[unmapped_mask, column_name].astype(str).str.lower().str.strip()
        df_clean.loc[unmapped_mask, column_name] = original_unmapped.apply(
            lambda x: 'sell' if x.startswith('s') else ('buy' if x.startswith('b') else None)
        )
    
    # Final check for any remaining null values
    remaining_nulls = df_clean[column_name].isnull().sum()
    if remaining_nulls > 0:
        logger.error(f"Still have {remaining_nulls} null values in {column_name} after standardization")
        # Drop rows with invalid Sell_Buy values as they're unusable
        df_clean = df_clean.dropna(subset=[column_name])
        logger.info(f"Dropped {remaining_nulls} rows with invalid {column_name} values")
    
    # Log the results
    final_values = df_clean[column_name].value_counts()
    logger.info(f"Standardized {column_name} values: {final_values.to_dict()}")
    
    return df_clean

def convert_timestamps_to_utc(df: pd.DataFrame, timestamp_column: str = 'Timestamp') -> pd.DataFrame:
    """
    Convert timestamp column to UTC timezone
    
    Args:
        df: DataFrame with timestamp data
        timestamp_column: Name of the timestamp column
        
    Returns:
        DataFrame with UTC timestamps
    """
    df_clean = df.copy()
    
    if timestamp_column not in df_clean.columns:
        logger.warning(f"Column '{timestamp_column}' not found in DataFrame")
        return df_clean
    
    logger.info(f"Converting {timestamp_column} to UTC...")
    
    try:
        # Parse timestamps with mixed timezones - use utc=True to handle mixed zones
        df_clean[timestamp_column] = pd.to_datetime(
            df_clean[timestamp_column], 
            utc=True,  # This handles mixed timezones by converting all to UTC
            errors='coerce'  # Convert unparseable dates to NaT
        )
        
        # Check for any parsing failures
        parsing_failures = df_clean[timestamp_column].isnull().sum()
        if parsing_failures > 0:
            logger.warning(f"Failed to parse {parsing_failures} timestamps")
        
        # Log timezone info
        logger.info(f"Successfully converted {len(df_clean) - parsing_failures} timestamps to UTC")
        
        # Show date range
        if not df_clean[timestamp_column].isnull().all():
            min_date = df_clean[timestamp_column].min()
            max_date = df_clean[timestamp_column].max()
            logger.info(f"Date range: {min_date} to {max_date}")
        
    except Exception as e:
        logger.error(f"Error converting timestamps to UTC: {str(e)}")
        # Fallback: try without UTC conversion
        try:
            df_clean[timestamp_column] = pd.to_datetime(df_clean[timestamp_column], errors='coerce')
            logger.warning("Fallback: parsed timestamps without UTC conversion")
        except Exception as e2:
            logger.error(f"Fallback parsing also failed: {str(e2)}")
            raise
    
    return df_clean

def detect_outliers(series: pd.Series, method: str = 'iqr', threshold: float = 1.5) -> pd.Series:
    """
    Detect outliers in a numerical series
    
    Args:
        series: Pandas series with numerical data
        method: Method for outlier detection ('iqr' or 'zscore')
        threshold: Threshold for outlier detection
        
    Returns:
        Boolean series indicating outliers
    """
    if series.empty or series.isnull().all():
        return pd.Series([False] * len(series), index=series.index)
    
    if method == 'iqr':
        # IQR method: outliers beyond Q1 - threshold*IQR or Q3 + threshold*IQR
        Q1 = series.quantile(0.25)
        Q3 = series.quantile(0.75)
        IQR = Q3 - Q1
        
        lower_bound = Q1 - threshold * IQR
        upper_bound = Q3 + threshold * IQR
        
        outliers = (series < lower_bound) | (series > upper_bound)
        
    elif method == 'zscore':
        # Z-score method: outliers beyond threshold standard deviations from mean
        mean = series.mean()
        std = series.std()
        
        if std == 0:  # Handle case where all values are the same
            outliers = pd.Series([False] * len(series), index=series.index)
        else:
            z_scores = np.abs((series - mean) / std)
            outliers = z_scores > threshold
    
    else:
        raise ValueError(f"Unknown outlier detection method: {method}")
    
    return outliers

def handle_missing_values(df: pd.DataFrame, strategy: str = 'median') -> pd.DataFrame:
    """
    Handle missing values in numerical columns
    
    Args:
        df: DataFrame with missing values
        strategy: Strategy for handling missing values ('median', 'mean', 'drop', 'forward_fill')
        
    Returns:
        DataFrame with missing values handled
    """
    df_clean = df.copy()
    
    numerical_cols = df_clean.select_dtypes(include=[np.number]).columns
    
    for col in numerical_cols:
        missing_count = df_clean[col].isnull().sum()
        if missing_count > 0:
            logger.info(f"Handling {missing_count} missing values in {col} using {strategy}")
            
            if strategy == 'median':
                fill_value = df_clean[col].median()
                df_clean[col].fillna(fill_value, inplace=True)
            elif strategy == 'mean':
                fill_value = df_clean[col].mean()
                df_clean[col].fillna(fill_value, inplace=True)
            elif strategy == 'forward_fill':
                df_clean[col].fillna(method='ffill', inplace=True)
                # Handle any remaining NaN at the beginning
                df_clean[col].fillna(df_clean[col].median(), inplace=True)
            elif strategy == 'drop':
                df_clean = df_clean.dropna(subset=[col])
            else:
                raise ValueError(f"Unknown missing value strategy: {strategy}")
    
    return df_clean

def handle_outliers(df: pd.DataFrame, columns: List[str], method: str = 'cap', detection_method: str = 'iqr') -> pd.DataFrame:
    """
    Handle outliers in specified columns
    
    Args:
        df: DataFrame with potential outliers
        columns: List of column names to process
        method: How to handle outliers ('cap', 'remove', 'flag')
        detection_method: How to detect outliers ('iqr', 'zscore')
        
    Returns:
        DataFrame with outliers handled
    """
    df_clean = df.copy()
    
    for col in columns:
        if col not in df_clean.columns:
            logger.warning(f"Column {col} not found, skipping outlier handling")
            continue
            
        outliers = detect_outliers(df_clean[col], method=detection_method)
        outlier_count = outliers.sum()
        
        if outlier_count > 0:
            logger.info(f"Found {outlier_count} outliers in {col}")
            
            if method == 'cap':
                # Cap outliers to reasonable bounds
                Q1 = df_clean[col].quantile(0.25)
                Q3 = df_clean[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                df_clean[col] = df_clean[col].clip(lower=lower_bound, upper=upper_bound)
                logger.info(f"Capped {col} outliers to range [{lower_bound:.2f}, {upper_bound:.2f}]")
                
            elif method == 'remove':
                df_clean = df_clean[~outliers]
                logger.info(f"Removed {outlier_count} outlier rows")
                
            elif method == 'flag':
                # Add a flag column for outliers
                flag_col = f"{col}_outlier_flag"
                df_clean[flag_col] = outliers
                logger.info(f"Added outlier flag column: {flag_col}")
    
    return df_clean

def calculate_vwap(prices: pd.Series, volumes: pd.Series) -> float:
    """
    Calculate Volume-Weighted Average Price (VWAP)
    
    Args:
        prices: Series of prices
        volumes: Series of volumes
        
    Returns:
        VWAP value
        
    Formula: VWAP = Σ(Price × Volume) / Σ(Volume)
    
    Example:
        >>> prices = pd.Series([100, 200, 150])
        >>> volumes = pd.Series([10, 5, 20])
        >>> vwap = calculate_vwap(prices, volumes)
        >>> # VWAP = (100*10 + 200*5 + 150*20) / (10+5+20) = 5000/35 = 142.86
    """
    if len(prices) == 0 or len(volumes) == 0:
        logger.warning("Empty price or volume series provided to VWAP calculation")
        return np.nan
    
    if len(prices) != len(volumes):
        logger.error(f"Price series length ({len(prices)}) != Volume series length ({len(volumes)})")
        return np.nan
    
    # Remove rows where either price or volume is missing
    valid_mask = ~(prices.isnull() | volumes.isnull())
    valid_prices = prices[valid_mask]
    valid_volumes = volumes[valid_mask]
    
    if len(valid_prices) == 0:
        logger.warning("No valid price-volume pairs for VWAP calculation")
        return np.nan
    
    # Check for zero or negative volumes (shouldn't happen in energy markets)
    if (valid_volumes <= 0).any():
        logger.warning("Found zero or negative volumes in VWAP calculation")
        # Filter out non-positive volumes
        positive_mask = valid_volumes > 0
        valid_prices = valid_prices[positive_mask]
        valid_volumes = valid_volumes[positive_mask]
        
        if len(valid_prices) == 0:
            logger.warning("No positive volumes for VWAP calculation")
            return np.nan
    
    # Calculate VWAP: Σ(Price × Volume) / Σ(Volume)
    total_value = (valid_prices * valid_volumes).sum()
    total_volume = valid_volumes.sum()
    
    if total_volume == 0:
        logger.warning("Total volume is zero, cannot calculate VWAP")
        return np.nan
    
    vwap = total_value / total_volume
    
    logger.debug(f"VWAP calculated: {vwap:.2f} from {len(valid_prices)} valid trades")
    return float(vwap)

def find_market_equilibrium(buy_orders: pd.DataFrame, sell_orders: pd.DataFrame) -> Dict:
    """
    Find market equilibrium point between buy and sell orders
    
    Args:
        buy_orders: DataFrame with buy orders (must have 'Price', 'Volume' columns)
        sell_orders: DataFrame with sell orders (must have 'Price', 'Volume' columns)
        
    Returns:
        Dictionary with equilibrium information
        
    Market clearing logic:
    1. Sort buy orders by price (descending) - demand curve
    2. Sort sell orders by price (ascending) - supply curve  
    3. Find intersection point where cumulative volumes meet
    4. Return equilibrium price and volume
    """
    equilibrium = {
        'equilibrium_price': None,
        'equilibrium_volume': 0,
        'total_buy_volume': buy_orders['Volume'].sum() if not buy_orders.empty else 0,
        'total_sell_volume': sell_orders['Volume'].sum() if not sell_orders.empty else 0,
        'buy_orders_count': len(buy_orders),
        'sell_orders_count': len(sell_orders)
    }
    
    # Check if we have both buy and sell orders
    if buy_orders.empty or sell_orders.empty:
        logger.warning("Cannot find equilibrium: missing buy or sell orders")
        return equilibrium
    
    # Validate required columns
    required_cols = ['Price', 'Volume']
    for df, name in [(buy_orders, 'buy_orders'), (sell_orders, 'sell_orders')]:
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.error(f"Missing columns in {name}: {missing_cols}")
            return equilibrium
    
    try:
        # 1. Sort buy orders by price (descending) - highest prices first (demand curve)
        buy_sorted = buy_orders.sort_values('Price', ascending=False).reset_index(drop=True)
        
        # 2. Sort sell orders by price (ascending) - lowest prices first (supply curve)
        sell_sorted = sell_orders.sort_values('Price', ascending=True).reset_index(drop=True)
        
        # 3. Calculate cumulative volumes
        buy_sorted['cumulative_volume'] = buy_sorted['Volume'].cumsum()
        sell_sorted['cumulative_volume'] = sell_sorted['Volume'].cumsum()
        
        # 4. Find intersection point
        # We need to find where the demand curve (buy) meets supply curve (sell)
        # This happens when cumulative buy volume >= cumulative sell volume
        # and the buy price >= sell price
        
        equilibrium_found = False
        best_equilibrium_price = None
        best_equilibrium_volume = 0
        
        # Check each possible intersection point
        for i, buy_row in buy_sorted.iterrows():
            buy_price = buy_row['Price']
            buy_cum_vol = buy_row['cumulative_volume']
            
            # Find sell orders that can be satisfied at this buy price or lower
            feasible_sells = sell_sorted[sell_sorted['Price'] <= buy_price]
            
            if not feasible_sells.empty:
                # Maximum volume that can be traded
                max_sell_volume = feasible_sells['cumulative_volume'].iloc[-1]
                
                # Equilibrium volume is the minimum of what buyers want and sellers offer
                equilibrium_volume = min(buy_cum_vol, max_sell_volume)
                
                if equilibrium_volume > best_equilibrium_volume:
                    best_equilibrium_volume = equilibrium_volume
                    
                    # Equilibrium price is typically the price of the marginal (last) order
                    # Find the sell price at the equilibrium volume
                    marginal_sell = feasible_sells[feasible_sells['cumulative_volume'] >= equilibrium_volume]
                    if not marginal_sell.empty:
                        best_equilibrium_price = marginal_sell.iloc[0]['Price']
                        equilibrium_found = True
        
        if equilibrium_found:
            equilibrium['equilibrium_price'] = float(best_equilibrium_price)
            equilibrium['equilibrium_volume'] = float(best_equilibrium_volume)
            
            # Add additional market insights
            equilibrium['market_efficiency'] = min(best_equilibrium_volume / equilibrium['total_buy_volume'],
                                                 best_equilibrium_volume / equilibrium['total_sell_volume'])
            
            # Price spread analysis
            min_sell_price = sell_sorted['Price'].min()
            max_buy_price = buy_sorted['Price'].max()
            equilibrium['bid_ask_spread'] = max_buy_price - min_sell_price
            
            logger.info(f"Market equilibrium found: Price=€{best_equilibrium_price:.2f}, Volume={best_equilibrium_volume:.0f} MWh")
        else:
            logger.warning("No market equilibrium found - no feasible trades")
            
    except Exception as e:
        logger.error(f"Error in market equilibrium calculation: {str(e)}")
    
    return equilibrium

def calculate_hourly_equilibrium(df: pd.DataFrame, timestamp_col: str = 'Timestamp') -> pd.DataFrame:
    """
    Calculate market equilibrium for each hour
    
    Args:
        df: DataFrame with energy trading data
        timestamp_col: Name of timestamp column
        
    Returns:
        DataFrame with hourly equilibrium prices and volumes
    """
    if df.empty:
        logger.warning("Empty DataFrame provided for equilibrium calculation")
        return pd.DataFrame()
    
    # Ensure timestamp is datetime
    if not pd.api.types.is_datetime64_any_dtype(df[timestamp_col]):
        df[timestamp_col] = pd.to_datetime(df[timestamp_col])
    
    # Create hour column for grouping
    df_with_hour = df.copy()
    df_with_hour['hour'] = df_with_hour[timestamp_col].dt.floor('h')
    
    equilibrium_results = []
    
    # Process each hour
    for hour, hour_data in df_with_hour.groupby('hour'):
        # Split into buy and sell orders
        buy_orders = hour_data[hour_data['Sell_Buy'] == 'buy'][['Price', 'Volume']].copy()
        sell_orders = hour_data[hour_data['Sell_Buy'] == 'sell'][['Price', 'Volume']].copy()
        
        # Calculate equilibrium for this hour
        equilibrium = find_market_equilibrium(buy_orders, sell_orders)
        
        # Add hour information
        equilibrium['hour'] = hour
        equilibrium_results.append(equilibrium)
    
    # Convert to DataFrame
    equilibrium_df = pd.DataFrame(equilibrium_results)
    
    # Reorder columns for better readability
    column_order = ['hour', 'equilibrium_price', 'equilibrium_volume', 
                   'total_buy_volume', 'total_sell_volume', 'buy_orders_count', 
                   'sell_orders_count', 'market_efficiency', 'bid_ask_spread']
    
    # Only include columns that exist
    available_columns = [col for col in column_order if col in equilibrium_df.columns]
    equilibrium_df = equilibrium_df[available_columns]
    
    logger.info(f"Calculated equilibrium for {len(equilibrium_df)} hours")
    return equilibrium_df

def simulate_renewable_shift(sell_orders: pd.DataFrame, shift_mwh: float) -> pd.DataFrame:
    """
    Simulate the impact of renewable energy changes on supply
    
    Args:
        sell_orders: DataFrame with sell orders
        shift_mwh: Amount to shift supply (+/- MWh)
                  Positive = more renewable energy (increase supply)
                  Negative = less renewable energy (decrease supply)
        
    Returns:
        DataFrame with adjusted sell volumes
        
    Logic:
    - Positive shift: Add renewable energy at low prices (shifts supply curve right)
    - Negative shift: Remove energy proportionally (shifts supply curve left)
    """
    if sell_orders.empty:
        logger.warning("Empty sell orders provided for renewable shift simulation")
        return sell_orders.copy()
    
    if 'Price' not in sell_orders.columns or 'Volume' not in sell_orders.columns:
        logger.error("Sell orders must have 'Price' and 'Volume' columns")
        return sell_orders.copy()
    
    adjusted_orders = sell_orders.copy()
    
    if shift_mwh == 0:
        logger.info("No renewable shift applied (shift_mwh = 0)")
        return adjusted_orders
    
    if shift_mwh > 0:
        # Positive shift: Add renewable energy
        # Renewable energy typically comes at very low marginal cost
        # Add it at the lowest price point to simulate renewable priority dispatch
        
        logger.info(f"Adding {shift_mwh} MWh of renewable energy to supply")
        
        # Find the lowest price in the market
        min_price = adjusted_orders['Price'].min()
        renewable_price = min_price * 0.1  # Renewable at 10% of lowest market price
        
        # Add renewable energy as a new sell order
        renewable_order = pd.DataFrame({
            'Price': [renewable_price],
            'Volume': [shift_mwh]
        })
        
        # Add any additional columns that might exist
        for col in adjusted_orders.columns:
            if col not in renewable_order.columns:
                renewable_order[col] = None
        
        adjusted_orders = pd.concat([adjusted_orders, renewable_order], ignore_index=True)
        
        logger.info(f"Added renewable energy at €{renewable_price:.2f}/MWh")
        
    else:
        # Negative shift: Remove energy (simulate renewable shortage)
        # Remove energy proportionally from all sell orders
        
        shift_mwh = abs(shift_mwh)  # Make positive for calculations
        total_volume = adjusted_orders['Volume'].sum()
        
        if shift_mwh >= total_volume:
            logger.warning(f"Renewable shortage ({shift_mwh} MWh) exceeds total supply ({total_volume} MWh)")
            # Remove all but a small amount to avoid empty market
            reduction_factor = 0.95  # Keep 5% of original supply
        else:
            reduction_factor = (total_volume - shift_mwh) / total_volume
        
        logger.info(f"Reducing supply by {shift_mwh} MWh (reduction factor: {reduction_factor:.3f})")
        
        # Apply proportional reduction to all sell orders
        adjusted_orders['Volume'] = adjusted_orders['Volume'] * reduction_factor
        
        # Remove orders that become too small (< 1 MWh)
        min_volume_threshold = 1.0
        before_count = len(adjusted_orders)
        adjusted_orders = adjusted_orders[adjusted_orders['Volume'] >= min_volume_threshold]
        after_count = len(adjusted_orders)
        
        if before_count != after_count:
            logger.info(f"Removed {before_count - after_count} orders below {min_volume_threshold} MWh threshold")
    
    # Log the impact
    original_volume = sell_orders['Volume'].sum()
    new_volume = adjusted_orders['Volume'].sum()
    volume_change = new_volume - original_volume
    
    logger.info(f"Supply volume changed from {original_volume:.0f} to {new_volume:.0f} MWh ({volume_change:+.0f} MWh)")
    
    return adjusted_orders

def calculate_renewable_impact_scenarios(df: pd.DataFrame, timestamp_col: str = 'Timestamp', 
                                       high_renewable_shift: float = 2000, 
                                       low_renewable_shift: float = -2000) -> pd.DataFrame:
    """
    Calculate equilibrium prices under different renewable energy scenarios
    
    Args:
        df: DataFrame with energy trading data
        timestamp_col: Name of timestamp column
        high_renewable_shift: MWh to add for high renewable scenario (positive)
        low_renewable_shift: MWh to remove for low renewable scenario (negative)
        
    Returns:
        DataFrame with baseline and scenario equilibrium prices
    """
    if df.empty:
        logger.warning("Empty DataFrame provided for renewable impact analysis")
        return pd.DataFrame()
    
    # Ensure timestamp is datetime
    if not pd.api.types.is_datetime64_any_dtype(df[timestamp_col]):
        df[timestamp_col] = pd.to_datetime(df[timestamp_col])
    
    # Create hour column for grouping
    df_with_hour = df.copy()
    df_with_hour['hour'] = df_with_hour[timestamp_col].dt.floor('h')
    
    scenario_results = []
    
    # Process each hour
    for hour, hour_data in df_with_hour.groupby('hour'):
        # Split into buy and sell orders
        buy_orders = hour_data[hour_data['Sell_Buy'] == 'buy'][['Price', 'Volume']].copy()
        sell_orders = hour_data[hour_data['Sell_Buy'] == 'sell'][['Price', 'Volume']].copy()
        
        # Scenario 1: Baseline (current market)
        baseline_equilibrium = find_market_equilibrium(buy_orders, sell_orders)
        
        # Scenario 2: High renewables (+2000 MWh)
        high_renewable_sells = simulate_renewable_shift(sell_orders, high_renewable_shift)
        high_renewable_equilibrium = find_market_equilibrium(buy_orders, high_renewable_sells)
        
        # Scenario 3: Low renewables (-2000 MWh)
        low_renewable_sells = simulate_renewable_shift(sell_orders, low_renewable_shift)
        low_renewable_equilibrium = find_market_equilibrium(buy_orders, low_renewable_sells)
        
        # Combine results
        scenario_result = {
            'hour': hour,
            'baseline_price': baseline_equilibrium['equilibrium_price'],
            'baseline_volume': baseline_equilibrium['equilibrium_volume'],
            'high_renewable_price': high_renewable_equilibrium['equilibrium_price'],
            'high_renewable_volume': high_renewable_equilibrium['equilibrium_volume'],
            'low_renewable_price': low_renewable_equilibrium['equilibrium_price'],
            'low_renewable_volume': low_renewable_equilibrium['equilibrium_volume']
        }
        
        # Calculate price impacts
        if baseline_equilibrium['equilibrium_price'] is not None:
            if high_renewable_equilibrium['equilibrium_price'] is not None:
                scenario_result['high_renewable_price_impact'] = (
                    high_renewable_equilibrium['equilibrium_price'] - baseline_equilibrium['equilibrium_price']
                )
                scenario_result['high_renewable_price_change_pct'] = (
                    scenario_result['high_renewable_price_impact'] / baseline_equilibrium['equilibrium_price'] * 100
                )
            
            if low_renewable_equilibrium['equilibrium_price'] is not None:
                scenario_result['low_renewable_price_impact'] = (
                    low_renewable_equilibrium['equilibrium_price'] - baseline_equilibrium['equilibrium_price']
                )
                scenario_result['low_renewable_price_change_pct'] = (
                    scenario_result['low_renewable_price_impact'] / baseline_equilibrium['equilibrium_price'] * 100
                )
        
        scenario_results.append(scenario_result)
    
    # Convert to DataFrame
    scenarios_df = pd.DataFrame(scenario_results)
    
    logger.info(f"Calculated renewable impact scenarios for {len(scenarios_df)} hours")
    return scenarios_df

def calculate_hourly_aggregations(df: pd.DataFrame, timestamp_col: str = 'Timestamp') -> pd.DataFrame:
    """
    Calculate hourly aggregations for buy and sell orders
    
    Args:
        df: DataFrame with energy trading data
        timestamp_col: Name of timestamp column
        
    Returns:
        DataFrame with hourly aggregations
        
    Calculates for each hour:
    - Buy orders: count, avg price, total volume, min/max price, VWAP
    - Sell orders: count, avg price, total volume, min/max price, VWAP
    - Market spread: difference between avg buy and sell prices
    """
    if df.empty:
        logger.warning("Empty DataFrame provided for hourly aggregations")
        return pd.DataFrame()
    
    # Ensure timestamp is datetime
    if not pd.api.types.is_datetime64_any_dtype(df[timestamp_col]):
        df[timestamp_col] = pd.to_datetime(df[timestamp_col])
    
    # Create hour column for grouping
    df_with_hour = df.copy()
    df_with_hour['hour'] = df_with_hour[timestamp_col].dt.floor('h')
    
    # Group by hour and sell_buy type
    grouped = df_with_hour.groupby(['hour', 'Sell_Buy'])
    
    # Calculate basic aggregations
    agg_results = grouped.agg({
        'Price': ['count', 'mean', 'min', 'max'],
        'Volume': ['sum', 'mean']
    }).round(2)
    
    # Flatten column names
    agg_results.columns = ['_'.join(col).strip() for col in agg_results.columns]
    agg_results = agg_results.reset_index()
    
    # Calculate VWAP for each group
    vwap_results = []
    for (hour, sell_buy), group in grouped:
        vwap = calculate_vwap(group['Price'], group['Volume'])
        vwap_results.append({
            'hour': hour,
            'Sell_Buy': sell_buy,
            'vwap': round(vwap, 2) if not np.isnan(vwap) else np.nan
        })
    
    vwap_df = pd.DataFrame(vwap_results)
    
    # Merge VWAP with other aggregations
    hourly_aggs = agg_results.merge(vwap_df, on=['hour', 'Sell_Buy'], how='left')
    
    # Pivot to get buy and sell metrics in separate columns
    pivot_df = hourly_aggs.pivot(index='hour', columns='Sell_Buy', values=[
        'Price_count', 'Price_mean', 'Price_min', 'Price_max',
        'Volume_sum', 'Volume_mean', 'vwap'
    ])
    
    # Flatten column names: metric_buy, metric_sell
    pivot_df.columns = [f"{metric}_{sell_buy}" for metric, sell_buy in pivot_df.columns]
    pivot_df = pivot_df.reset_index()
    
    # Calculate market spread (difference between avg buy and sell prices)
    if 'Price_mean_buy' in pivot_df.columns and 'Price_mean_sell' in pivot_df.columns:
        pivot_df['market_spread'] = (pivot_df['Price_mean_buy'] - pivot_df['Price_mean_sell']).round(2)
    else:
        pivot_df['market_spread'] = np.nan
        logger.warning("Cannot calculate market spread - missing buy or sell price data")
    
    # Rename columns to match expected output format
    column_mapping = {
        'Price_count_buy': 'buy_count',
        'Price_count_sell': 'sell_count',
        'Price_mean_buy': 'buy_avg_price',
        'Price_mean_sell': 'sell_avg_price',
        'Price_min_buy': 'buy_min_price',
        'Price_max_buy': 'buy_max_price',
        'Price_min_sell': 'sell_min_price',
        'Price_max_sell': 'sell_max_price',
        'Volume_sum_buy': 'buy_total_volume',
        'Volume_sum_sell': 'sell_total_volume',
        'Volume_mean_buy': 'buy_avg_volume',
        'Volume_mean_sell': 'sell_avg_volume',
        'vwap_buy': 'buy_vwap',
        'vwap_sell': 'sell_vwap'
    }
    
    pivot_df = pivot_df.rename(columns=column_mapping)
    
    # Fill NaN values with 0 for counts and volumes (no trades = 0)
    count_volume_cols = ['buy_count', 'sell_count', 'buy_total_volume', 'sell_total_volume']
    for col in count_volume_cols:
        if col in pivot_df.columns:
            pivot_df[col] = pivot_df[col].fillna(0)
    
    logger.info(f"Calculated hourly aggregations for {len(pivot_df)} hours")
    return pivot_df

def detect_timestamp_gaps(df: pd.DataFrame, timestamp_col: str = 'Timestamp', 
                         max_gap_minutes: int = 60) -> Dict:
    """
    Detect gaps in timestamp data that exceed the specified threshold
    
    Args:
        df: DataFrame with timestamp data
        timestamp_col: Name of timestamp column
        max_gap_minutes: Maximum allowed gap in minutes
        
    Returns:
        Dictionary with gap analysis results
    """
    gap_analysis = {
        'gaps_detected': False,
        'total_gaps': 0,
        'max_gap_minutes': 0,
        'gap_details': [],
        'data_continuity_score': 100.0
    }
    
    if df.empty or timestamp_col not in df.columns:
        logger.warning(f"Cannot analyze timestamp gaps: empty data or missing {timestamp_col} column")
        return gap_analysis
    
    # Ensure timestamp column is datetime
    if not pd.api.types.is_datetime64_any_dtype(df[timestamp_col]):
        df[timestamp_col] = pd.to_datetime(df[timestamp_col])
    
    # Sort by timestamp
    df_sorted = df.sort_values(timestamp_col).reset_index(drop=True)
    
    # Calculate time differences between consecutive records
    time_diffs = df_sorted[timestamp_col].diff()
    
    # Convert to minutes
    time_diffs_minutes = time_diffs.dt.total_seconds() / 60
    
    # Find gaps exceeding threshold
    gap_mask = time_diffs_minutes > max_gap_minutes
    gaps = time_diffs_minutes[gap_mask]
    
    if len(gaps) > 0:
        gap_analysis['gaps_detected'] = True
        gap_analysis['total_gaps'] = len(gaps)
        gap_analysis['max_gap_minutes'] = float(gaps.max())
        
        # Create detailed gap information
        gap_indices = gaps.index
        for idx in gap_indices:
            if idx > 0:  # Skip first record (no previous timestamp)
                gap_start = df_sorted.loc[idx-1, timestamp_col]
                gap_end = df_sorted.loc[idx, timestamp_col]
                gap_duration = gaps.loc[idx]
                
                gap_analysis['gap_details'].append({
                    'gap_start': str(gap_start),
                    'gap_end': str(gap_end),
                    'gap_duration_minutes': float(gap_duration),
                    'records_before_gap': idx,
                    'records_after_gap': len(df_sorted) - idx
                })
        
        # Calculate data continuity score (percentage of time covered)
        total_time_span = (df_sorted[timestamp_col].max() - df_sorted[timestamp_col].min()).total_seconds() / 60
        total_gap_time = gaps.sum()
        
        if total_time_span > 0:
            gap_analysis['data_continuity_score'] = max(0, (total_time_span - total_gap_time) / total_time_span * 100)
        
        logger.warning(f"Detected {len(gaps)} timestamp gaps exceeding {max_gap_minutes} minutes")
        logger.warning(f"Largest gap: {gaps.max():.1f} minutes")
        logger.warning(f"Data continuity score: {gap_analysis['data_continuity_score']:.1f}%")
    else:
        logger.info(f"No timestamp gaps detected (threshold: {max_gap_minutes} minutes)")
    
    return gap_analysis

def validate_pipeline_health(validation_results: Dict, aggregation_results: Dict, 
                           equilibrium_results: Dict, renewable_results: Dict) -> Dict:
    """
    Comprehensive pipeline health validation
    
    Args:
        validation_results: Results from data validation task
        aggregation_results: Results from hourly aggregation task
        equilibrium_results: Results from market equilibrium task
        renewable_results: Results from renewable impact task
        
    Returns:
        Dictionary with overall pipeline health assessment
    """
    health_check = {
        'pipeline_healthy': True,
        'health_score': 100.0,
        'critical_issues': [],
        'warnings': [],
        'task_status': {},
        'data_quality_metrics': {},
        'performance_metrics': {},
        'recommendations': []
    }
    
    # Check each task's health
    tasks = {
        'data_validation': validation_results,
        'hourly_aggregation': aggregation_results,
        'market_equilibrium': equilibrium_results,
        'renewable_impact': renewable_results
    }
    
    for task_name, results in tasks.items():
        if results is None:
            health_check['critical_issues'].append(f"Task {task_name} failed to execute")
            health_check['task_status'][task_name] = 'FAILED'
            health_check['pipeline_healthy'] = False
            health_check['health_score'] -= 25
        else:
            health_check['task_status'][task_name] = 'SUCCESS'
    
    # Data quality assessment
    if validation_results:
        if not validation_results.get('validation_passed', True):
            health_check['critical_issues'].append("Data validation failed")
            health_check['pipeline_healthy'] = False
            health_check['health_score'] -= 30
        
        # Check missing data percentages
        missing_percentages = validation_results.get('missing_percentages', {})
        for col, pct in missing_percentages.items():
            if col in ['Price', 'Volume'] and pct > 10:
                health_check['critical_issues'].append(f"High missing data in {col}: {pct}%")
                health_check['pipeline_healthy'] = False
            elif pct > 5:
                health_check['warnings'].append(f"Moderate missing data in {col}: {pct}%")
        
        health_check['data_quality_metrics'] = {
            'missing_data_percentages': missing_percentages,
            'outlier_counts': validation_results.get('outliers', {}),
            'business_logic_issues': validation_results.get('business_logic_issues', [])
        }
    
    # Performance metrics
    if aggregation_results:
        total_hours = aggregation_results.get('total_hours_processed', 0)
        if total_hours == 0:
            health_check['warnings'].append("No hourly aggregations generated")
            health_check['health_score'] -= 10
        
        health_check['performance_metrics']['hours_processed'] = total_hours
    
    if equilibrium_results:
        success_rate = equilibrium_results.get('equilibrium_success_rate', 0)
        if success_rate < 50:
            health_check['warnings'].append(f"Low equilibrium success rate: {success_rate:.1f}%")
            health_check['health_score'] -= 15
        elif success_rate < 80:
            health_check['warnings'].append(f"Moderate equilibrium success rate: {success_rate:.1f}%")
            health_check['health_score'] -= 5
        
        health_check['performance_metrics']['equilibrium_success_rate'] = success_rate
    
    # Generate recommendations
    if health_check['critical_issues']:
        health_check['recommendations'].append("Address critical issues before proceeding to production")
    
    if len(health_check['warnings']) > 3:
        health_check['recommendations'].append("Review data quality and consider additional cleaning steps")
    
    if health_check['health_score'] < 80:
        health_check['recommendations'].append("Pipeline health below acceptable threshold - investigate issues")
    
    # Final health score adjustment
    health_check['health_score'] = max(0, health_check['health_score'])
    
    logger.info(f"Pipeline health assessment complete: {health_check['health_score']:.1f}% healthy")
    
    return health_check

def create_output_directories(base_path: str) -> None:
    """
    Create necessary output directories for the pipeline
    
    Args:
        base_path: Base directory path for outputs
    """
    directories = [
        f"{base_path}/processed_data",
        f"{base_path}/output", 
        f"{base_path}/reports",
        f"{base_path}/logs"
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        logger.info(f"Created directory: {directory}")

def save_parquet_by_day(df: pd.DataFrame, output_dir: str, timestamp_column: str = 'Timestamp', 
                       lineage_info: Optional[Dict] = None) -> List[str]:
    """
    Partition DataFrame by day and save as Parquet files with lineage information
    
    Args:
        df: DataFrame to partition
        output_dir: Directory to save Parquet files
        timestamp_column: Name of timestamp column for partitioning
        lineage_info: Data lineage information to include with each partition
        
    Returns:
        List of saved file paths
    """
    saved_files = []
    
    if df.empty:
        logger.warning("DataFrame is empty, no files to save")
        return saved_files
    
    if timestamp_column not in df.columns:
        logger.error(f"Timestamp column '{timestamp_column}' not found in DataFrame")
        return saved_files
    
    # Ensure timestamp column is datetime
    if not pd.api.types.is_datetime64_any_dtype(df[timestamp_column]):
        logger.warning(f"Converting {timestamp_column} to datetime")
        df[timestamp_column] = pd.to_datetime(df[timestamp_column])
    
    # Create date column for partitioning
    df_with_date = df.copy()
    df_with_date['partition_date'] = df_with_date[timestamp_column].dt.date
    
    # Group by date and save each partition
    for date, group_df in df_with_date.groupby('partition_date'):
        # Create directory structure: processed_data/YYYY-MM-DD/
        date_str = date.strftime('%Y-%m-%d')
        partition_dir = os.path.join(output_dir, date_str)
        os.makedirs(partition_dir, exist_ok=True)
        
        # Remove the temporary partition_date column
        partition_data = group_df.drop('partition_date', axis=1)
        
        # Save data as Parquet
        data_file_path = os.path.join(partition_dir, 'data.parquet')
        partition_data.to_parquet(data_file_path, index=False)
        saved_files.append(data_file_path)
        
        # Create lineage metadata for this partition
        partition_lineage = {
            'partition_date': date_str,
            'record_count': len(partition_data),
            'columns': list(partition_data.columns),
            'data_types': partition_data.dtypes.astype(str).to_dict(),
            'timestamp_range': {
                'start': str(partition_data[timestamp_column].min()),
                'end': str(partition_data[timestamp_column].max())
            },
            'file_path': data_file_path,
            'created_at': pd.Timestamp.now().isoformat(),
            'partition_summary': {
                'buy_orders': len(partition_data[partition_data['Sell_Buy'] == 'buy']),
                'sell_orders': len(partition_data[partition_data['Sell_Buy'] == 'sell']),
                'price_range': {
                    'min': float(partition_data['Price'].min()),
                    'max': float(partition_data['Price'].max()),
                    'mean': float(partition_data['Price'].mean())
                },
                'volume_range': {
                    'min': float(partition_data['Volume'].min()),
                    'max': float(partition_data['Volume'].max()),
                    'total': float(partition_data['Volume'].sum())
                }
            }
        }
        
        # Include upstream lineage information if provided
        if lineage_info:
            partition_lineage['upstream_lineage'] = lineage_info
        
        # Save lineage metadata
        lineage_file_path = os.path.join(partition_dir, 'lineage.json')
        with open(lineage_file_path, 'w') as f:
            json.dump(partition_lineage, f, indent=2, default=str)
        
        logger.info(f"Saved partition {date_str}: {len(partition_data)} records to {data_file_path}")
    
    logger.info(f"Successfully partitioned data into {len(saved_files)} daily files")
    return saved_files
