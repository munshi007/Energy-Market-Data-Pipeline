"""
Utility functions for energy data processing
Terra One Student Data Engineer Challenge

These helper functions provide guidance and structure for your pipeline implementation.
You can modify or extend these as needed for your solution.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import logging
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
    
    # TODO: Add more validation checks here
    # - Check data types
    # - Check for completely empty columns
    # - Validate timestamp format
    
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
        'duplicate_rows': df.duplicated().sum(),
        'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024**2,
    }
    
    # TODO: Add more quality checks:
    # - Outlier detection for numerical columns
    # - Consistency checks for categorical columns
    # - Timestamp range validation
    # - Business logic validation (e.g., positive prices/volumes)
    
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
    
    # TODO: Implement standardization logic
    # Handle variations like: 'SELL', 'Sell', 'sell', 'BUY', 'Buy', 'buy'
    # Convert all to lowercase 'sell' or 'buy'
    # Handle any edge cases or invalid values
    
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
    
    # TODO: Implement timestamp conversion
    # - Parse timestamps with mixed timezones
    # - Convert to UTC
    # - Handle parsing errors gracefully
    
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
    # TODO: Implement outlier detection
    # - IQR method: outliers beyond Q1 - 1.5*IQR or Q3 + 1.5*IQR
    # - Z-score method: outliers beyond threshold standard deviations
    
    return pd.Series([False] * len(series), index=series.index)

def calculate_vwap(prices: pd.Series, volumes: pd.Series) -> float:
    """
    Calculate Volume-Weighted Average Price (VWAP)
    
    Args:
        prices: Series of prices
        volumes: Series of volumes
        
    Returns:
        VWAP value
        
    Formula: VWAP = Σ(Price × Volume) / Σ(Volume)
    """
    # TODO: Implement VWAP calculation
    # - Handle missing values
    # - Validate that price and volume series align
    # - Return NaN if no valid data
    
    return 0.0

def find_market_equilibrium(buy_orders: pd.DataFrame, sell_orders: pd.DataFrame) -> Dict:
    """
    Find market equilibrium point between buy and sell orders
    
    Args:
        buy_orders: DataFrame with buy orders (must have 'Price', 'Volume' columns)
        sell_orders: DataFrame with sell orders (must have 'Price', 'Volume' columns)
        
    Returns:
        Dictionary with equilibrium information
    """
    equilibrium = {
        'equilibrium_price': None,
        'equilibrium_volume': 0,
        'total_buy_volume': buy_orders['Volume'].sum() if not buy_orders.empty else 0,
        'total_sell_volume': sell_orders['Volume'].sum() if not sell_orders.empty else 0,
    }
    
    # TODO: Implement market clearing logic
    # 1. Sort buy orders by price (descending) - demand curve
    # 2. Sort sell orders by price (ascending) - supply curve  
    # 3. Find intersection point where cumulative volumes meet
    # 4. Return equilibrium price and volume
    
    return equilibrium

def simulate_renewable_shift(sell_orders: pd.DataFrame, shift_mwh: float) -> pd.DataFrame:
    """
    Simulate the impact of renewable energy changes on supply
    
    Args:
        sell_orders: DataFrame with sell orders
        shift_mwh: Amount to shift supply (+/- MWh)
        
    Returns:
        DataFrame with adjusted sell volumes
    """
    # TODO: Implement renewable shift simulation
    # - Distribute the shift proportionally across all sell orders
    # - Ensure volumes remain positive
    # - Handle edge cases
    
    return sell_orders.copy()

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

def save_parquet_by_day(df: pd.DataFrame, output_dir: str, timestamp_column: str = 'Timestamp') -> List[str]:
    """
    Partition DataFrame by day and save as Parquet files
    
    Args:
        df: DataFrame to partition
        output_dir: Directory to save Parquet files
        timestamp_column: Name of timestamp column for partitioning
        
    Returns:
        List of saved file paths
    """
    saved_files = []
    
    # TODO: Implement daily partitioning
    # - Group data by date
    # - Save each day as separate Parquet file
    # - Include metadata about partitioning
    
    return saved_files
