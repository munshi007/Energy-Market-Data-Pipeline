"""
Energy Market Data Pipeline - Apache Airflow Implementation

Comprehensive data engineering pipeline for processing energy market bid data,
featuring market clearing simulation and renewable energy impact analysis.

Author: Rohan Munshi
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import numpy as np
import logging
import os
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default DAG arguments
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize DAG
dag = DAG(
    'energy_data_pipeline',
    default_args=default_args,
    description='Energy market data processing pipeline with renewable impact analysis',
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=['energy-data', 'market-analysis', 'renewable-energy'],
)

# Data paths - configurable for different environments
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
RAW_DATA_PATH = os.path.join(AIRFLOW_HOME, 'dags', 'energy_data.csv')
PROCESSED_DATA_DIR = os.path.join(AIRFLOW_HOME, 'processed_data')
OUTPUT_DIR = os.path.join(AIRFLOW_HOME, 'output')

def test_environment(**context):
    """
    Test function to verify the Airflow environment is working
    """
    logger.info("Testing Airflow environment...")
    
    # Test basic imports
    try:
        import pandas as pd
        import numpy as np
        logger.info("✅ pandas and numpy imported successfully")
    except ImportError as e:
        logger.error(f"❌ Failed to import basic packages: {e}")
        raise
    
    # Test file access
    try:
        if os.path.exists(RAW_DATA_PATH):
            logger.info(f"✅ Raw data file found: {RAW_DATA_PATH}")
        else:
            logger.error(f"❌ Raw data file not found: {RAW_DATA_PATH}")
            raise FileNotFoundError(f"Raw data file not found: {RAW_DATA_PATH}")
    except Exception as e:
        logger.error(f"❌ File access error: {e}")
        raise
    
    # Test directory creation
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        logger.info(f"✅ Output directory created: {OUTPUT_DIR}")
    except Exception as e:
        logger.error(f"❌ Directory creation failed: {e}")
        raise
    
    logger.info("🎉 Environment test completed successfully!")
    return "Environment test passed"

def validate_data_quality(**context):
    """
    Task 1: Data Validation & Quality Check
    """
    logger.info("Starting data quality validation...")
    
    try:
        # Load the raw data
        logger.info(f"Loading data from: {RAW_DATA_PATH}")
        df = pd.read_csv(RAW_DATA_PATH)
        logger.info(f"Successfully loaded {len(df)} records")
        
        # Import our helper function
        import sys
        import os
        sys.path.append(AIRFLOW_HOME)
        sys.path.append(os.path.join(AIRFLOW_HOME, 'dags'))
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        
        try:
            from utils.data_helpers import generate_data_quality_report, validate_data_schema
        except ImportError as e:
            logger.error(f"Failed to import helper functions: {e}")
            # Fallback: try importing from current directory
            import importlib.util
            utils_path = os.path.join(AIRFLOW_HOME, 'utils', 'data_helpers.py')
            spec = importlib.util.spec_from_file_location("data_helpers", utils_path)
            data_helpers = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(data_helpers)
            generate_data_quality_report = data_helpers.generate_data_quality_report
            validate_data_schema = data_helpers.validate_data_schema
        
        # Validate schema first
        required_columns = ['Timestamp', 'Price', 'Volume', 'Sell_Buy']
        schema_validation = validate_data_schema(df, required_columns)
        
        if not schema_validation['schema_valid']:
            logger.error(f"Schema validation failed: {schema_validation['issues']}")
            raise ValueError(f"Schema validation failed: {schema_validation['issues']}")
        
        # Generate quality report
        quality_report = generate_data_quality_report(df)
        
        # Define validation thresholds
        MAX_MISSING_PERCENTAGE = 10.0  # Fail if >10% missing in critical fields
        MAX_OUTLIER_PERCENTAGE = 20.0  # Warning if >20% outliers
        
        # Check validation criteria
        validation_issues = []
        validation_passed = True
        
        # Check missing data thresholds
        for col, missing_pct in quality_report['missing_percentages'].items():
            if col in ['Price', 'Volume'] and missing_pct > MAX_MISSING_PERCENTAGE:
                validation_issues.append(f"Column '{col}' has {missing_pct}% missing values (threshold: {MAX_MISSING_PERCENTAGE}%)")
                validation_passed = False
        
        # Check for business logic issues
        if quality_report['business_logic_issues']:
            validation_issues.extend(quality_report['business_logic_issues'])
            # Don't fail for business issues, just warn
            logger.warning(f"Business logic issues found: {quality_report['business_logic_issues']}")
        
        # Add validation results to report
        quality_report['validation_passed'] = validation_passed
        quality_report['validation_issues'] = validation_issues
        quality_report['validation_thresholds'] = {
            'max_missing_percentage': MAX_MISSING_PERCENTAGE,
            'max_outlier_percentage': MAX_OUTLIER_PERCENTAGE
        }
        
        # Save quality report to file
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        report_path = f"{OUTPUT_DIR}/data_quality_report.json"
        
        import json
        with open(report_path, 'w') as f:
            # Convert numpy types to native Python types for JSON serialization
            json_report = json.loads(json.dumps(quality_report, default=str))
            json.dump(json_report, f, indent=2)
        
        logger.info(f"Quality report saved to: {report_path}")
        
        # Log summary
        logger.info("=== DATA QUALITY SUMMARY ===")
        logger.info(f"Total records: {quality_report['total_records']}")
        logger.info(f"Missing data: {quality_report['missing_percentages']}")
        logger.info(f"Outliers detected: {quality_report['outliers']}")
        logger.info(f"Validation passed: {validation_passed}")
        
        if validation_issues:
            logger.error(f"Validation issues: {validation_issues}")
        
        # Fail the task if validation didn't pass
        if not validation_passed:
            raise ValueError(f"Data quality validation failed: {validation_issues}")
        
        logger.info("Data quality validation completed successfully")
        
        # Convert numpy types to native Python types for JSON serialization
        def convert_numpy_types(obj):
            if isinstance(obj, dict):
                return {k: convert_numpy_types(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_numpy_types(v) for v in obj]
            elif hasattr(obj, 'item'):  # numpy scalar
                return obj.item()
            elif hasattr(obj, 'tolist'):  # numpy array
                return obj.tolist()
            else:
                return obj
        
        return convert_numpy_types(quality_report)
        
    except Exception as e:
        logger.error(f"Data quality validation failed: {str(e)}")
        raise

def clean_and_standardize_data(**context):
    """
    Task 2: Data Cleaning & Standardization
    """
    logger.info("Starting data cleaning and standardization...")
    
    try:
        # Load raw data
        logger.info(f"Loading raw data from: {RAW_DATA_PATH}")
        df = pd.read_csv(RAW_DATA_PATH)
        original_count = len(df)
        logger.info(f"Loaded {original_count} records for cleaning")
        
        # Import helper functions
        import sys
        import os
        sys.path.append(AIRFLOW_HOME)
        sys.path.append('/opt/airflow/dags')
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        
        try:
            from utils.data_helpers import (
                standardize_sell_buy_column, 
                convert_timestamps_to_utc,
                handle_missing_values,
                handle_outliers
            )
        except ImportError as e:
            logger.error(f"Import error: {e}")
            raise
        
        # Step 1: Standardize Sell_Buy column
        logger.info("Step 1: Standardizing Sell_Buy column...")
        df = standardize_sell_buy_column(df, 'Sell_Buy')
        
        # Step 2: Convert timestamps to UTC
        logger.info("Step 2: Converting timestamps to UTC...")
        df = convert_timestamps_to_utc(df, 'Timestamp')
        
        # Step 3: Handle missing values
        logger.info("Step 3: Handling missing values...")
        # Use median for Price and Volume as they're likely to be skewed
        df = handle_missing_values(df, strategy='median')
        
        # Step 4: Handle outliers
        logger.info("Step 4: Handling outliers...")
        # For energy data, we'll cap extreme outliers but keep high prices (they might be real)
        # Use more conservative outlier detection for prices
        df = handle_outliers(df, columns=['Price'], method='flag', detection_method='iqr')
        df = handle_outliers(df, columns=['Volume'], method='cap', detection_method='iqr')
        
        # Step 5: Additional data quality improvements
        logger.info("Step 5: Additional quality improvements...")
        
        # Remove any rows with invalid timestamps
        invalid_timestamps = df['Timestamp'].isnull()
        if invalid_timestamps.any():
            invalid_count = invalid_timestamps.sum()
            df = df[~invalid_timestamps]
            logger.warning(f"Removed {invalid_count} rows with invalid timestamps")
        
        # Ensure positive prices and volumes (business rule)
        negative_prices = df['Price'] < 0
        if negative_prices.any():
            logger.warning(f"Found {negative_prices.sum()} negative prices, setting to 0")
            df.loc[negative_prices, 'Price'] = 0
        
        negative_volumes = df['Volume'] < 0
        if negative_volumes.any():
            logger.warning(f"Found {negative_volumes.sum()} negative volumes, setting to 0")
            df.loc[negative_volumes, 'Volume'] = 0
        
        # Step 6: Sort by timestamp for better downstream processing
        df = df.sort_values('Timestamp').reset_index(drop=True)
        
        # Step 7: Create cleaning metadata
        cleaning_metadata = {
            'original_record_count': original_count,
            'final_record_count': len(df),
            'records_removed': original_count - len(df),
            'cleaning_steps_applied': [
                'Standardized Sell_Buy column',
                'Converted timestamps to UTC',
                'Handled missing values with median imputation',
                'Flagged price outliers',
                'Capped volume outliers',
                'Removed invalid timestamps',
                'Ensured positive prices and volumes',
                'Sorted by timestamp'
            ],
            'data_quality_summary': {
                'missing_values_after_cleaning': df.isnull().sum().to_dict(),
                'sell_buy_distribution': df['Sell_Buy'].value_counts().to_dict(),
                'date_range': {
                    'start': str(df['Timestamp'].min()),
                    'end': str(df['Timestamp'].max())
                }
            }
        }
        
        # Step 8: Save cleaned data
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        cleaned_data_path = f"{OUTPUT_DIR}/cleaned_data.csv"
        df.to_csv(cleaned_data_path, index=False)
        logger.info(f"Cleaned data saved to: {cleaned_data_path}")
        
        # Save cleaning metadata
        import json
        metadata_path = f"{OUTPUT_DIR}/cleaning_metadata.json"
        with open(metadata_path, 'w') as f:
            json.dump(cleaning_metadata, f, indent=2, default=str)
        logger.info(f"Cleaning metadata saved to: {metadata_path}")
        
        # Log summary
        logger.info("=== DATA CLEANING SUMMARY ===")
        logger.info(f"Original records: {original_count}")
        logger.info(f"Final records: {len(df)}")
        logger.info(f"Records removed: {original_count - len(df)}")
        logger.info(f"Missing values remaining: {df.isnull().sum().sum()}")
        logger.info(f"Sell_Buy distribution: {df['Sell_Buy'].value_counts().to_dict()}")
        
        logger.info("Data cleaning and standardization completed successfully")
        return cleaning_metadata
        
    except Exception as e:
        logger.error(f"Data cleaning failed: {str(e)}")
        raise

def partition_and_store_data(**context):
    """
    Task 3: Data Partitioning & Storage
    """
    logger.info("Starting data partitioning and storage...")
    
    try:
        # Import required modules
        import os
        import json
        import pandas as pd
        import sys
        
        # Add path for helper functions
        sys.path.append(AIRFLOW_HOME)
        sys.path.append('/opt/airflow/dags')
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        
        try:
            from utils.data_helpers import save_parquet_by_day
        except ImportError as e:
            logger.error(f"Failed to import helper functions: {e}")
            # Fallback: try importing from current directory
            import importlib.util
            spec = importlib.util.spec_from_file_location("data_helpers", "/opt/airflow/utils/data_helpers.py")
            data_helpers = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(data_helpers)
            save_parquet_by_day = data_helpers.save_parquet_by_day
        # Load cleaned data from previous task
        cleaned_data_path = f"{OUTPUT_DIR}/cleaned_data.csv"
        
        if not os.path.exists(cleaned_data_path):
            raise FileNotFoundError(f"Cleaned data not found at {cleaned_data_path}")
        
        logger.info(f"Loading cleaned data from: {cleaned_data_path}")
        df = pd.read_csv(cleaned_data_path)
        
        # Convert timestamp back to datetime (CSV loses the datetime type)
        df['Timestamp'] = pd.to_datetime(df['Timestamp'])
        
        logger.info(f"Loaded {len(df)} cleaned records for partitioning")
        
        # Load cleaning metadata for lineage tracking
        metadata_path = f"{OUTPUT_DIR}/cleaning_metadata.json"
        lineage_info = {}
        
        if os.path.exists(metadata_path):
            with open(metadata_path, 'r') as f:
                cleaning_metadata = json.load(f)
                lineage_info = {
                    'source_file': RAW_DATA_PATH,
                    'cleaning_metadata': cleaning_metadata,
                    'processing_timestamp': pd.Timestamp.now().isoformat()
                }
        
        # Partitioning function already imported above
        
        # Create processed data directory
        os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
        
        # Partition and save data
        logger.info("Partitioning data by day...")
        saved_files = save_parquet_by_day(
            df=df,
            output_dir=PROCESSED_DATA_DIR,
            timestamp_column='Timestamp',
            lineage_info=lineage_info
        )
        
        if not saved_files:
            raise ValueError("No files were saved during partitioning")
        
        # Create summary of partitioning results
        partitioning_summary = {
            'total_records_partitioned': len(df),
            'partitions_created': len(saved_files),
            'partition_files': saved_files,
            'partitioning_timestamp': pd.Timestamp.now().isoformat(),
            'date_range': {
                'start': str(df['Timestamp'].min().date()),
                'end': str(df['Timestamp'].max().date())
            }
        }
        
        # Analyze partitions
        partition_analysis = {}
        for file_path in saved_files:
            # Extract date from path
            date_str = os.path.basename(os.path.dirname(file_path))
            
            # Load partition to get stats
            partition_df = pd.read_parquet(file_path)
            partition_analysis[date_str] = {
                'record_count': len(partition_df),
                'buy_orders': len(partition_df[partition_df['Sell_Buy'] == 'buy']),
                'sell_orders': len(partition_df[partition_df['Sell_Buy'] == 'sell']),
                'file_size_mb': round(os.path.getsize(file_path) / (1024*1024), 2)
            }
        
        partitioning_summary['partition_analysis'] = partition_analysis
        
        # Save partitioning summary
        summary_path = f"{OUTPUT_DIR}/partitioning_summary.json"
        with open(summary_path, 'w') as f:
            json.dump(partitioning_summary, f, indent=2, default=str)
        
        logger.info(f"Partitioning summary saved to: {summary_path}")
        
        # Log results
        logger.info("=== DATA PARTITIONING SUMMARY ===")
        logger.info(f"Total records partitioned: {len(df)}")
        logger.info(f"Partitions created: {len(saved_files)}")
        logger.info(f"Date range: {partitioning_summary['date_range']['start']} to {partitioning_summary['date_range']['end']}")
        
        for date_str, stats in partition_analysis.items():
            logger.info(f"Partition {date_str}: {stats['record_count']} records ({stats['buy_orders']} buy, {stats['sell_orders']} sell)")
        
        logger.info("Data partitioning and storage completed successfully")
        return partitioning_summary
        
    except Exception as e:
        logger.error(f"Data partitioning failed: {str(e)}")
        raise

def calculate_hourly_aggregations(**context):
    """
    Task 4: Hourly Aggregations
    """
    logger.info("Calculating hourly aggregations...")
    
    try:
        # Import required modules
        import sys
        import os
        import json
        import pandas as pd
        
        # Import helper function
        sys.path.append(AIRFLOW_HOME)
        sys.path.append('/opt/airflow/dags')
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        
        try:
            from utils.data_helpers import calculate_hourly_aggregations
        except ImportError as e:
            logger.error(f"Failed to import helper functions: {e}")
            # Fallback: try importing from current directory
            import importlib.util
            spec = importlib.util.spec_from_file_location("data_helpers", "/opt/airflow/utils/data_helpers.py")
            data_helpers = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(data_helpers)
            calculate_hourly_aggregations = data_helpers.calculate_hourly_aggregations
        
        # Load partitioned data and process each day
        if not os.path.exists(PROCESSED_DATA_DIR):
            raise FileNotFoundError(f"Processed data directory not found: {PROCESSED_DATA_DIR}")
        
        # Find all partition directories
        partition_dirs = [d for d in os.listdir(PROCESSED_DATA_DIR) 
                         if os.path.isdir(os.path.join(PROCESSED_DATA_DIR, d))]
        
        if not partition_dirs:
            raise ValueError("No partition directories found")
        
        logger.info(f"Found {len(partition_dirs)} partitions to process")
        
        all_hourly_aggs = []
        partition_summaries = {}
        
        # Process each partition
        for partition_date in sorted(partition_dirs):
            partition_path = os.path.join(PROCESSED_DATA_DIR, partition_date)
            data_file = os.path.join(partition_path, 'data.parquet')
            
            if not os.path.exists(data_file):
                logger.warning(f"Data file not found for partition {partition_date}")
                continue
            
            logger.info(f"Processing partition: {partition_date}")
            
            # Load partition data
            df = pd.read_parquet(data_file)
            logger.info(f"Loaded {len(df)} records from {partition_date}")
            
            # Calculate hourly aggregations for this partition
            hourly_aggs = calculate_hourly_aggregations(df, 'Timestamp')
            
            if not hourly_aggs.empty:
                # Add partition date for tracking
                hourly_aggs['date'] = partition_date
                all_hourly_aggs.append(hourly_aggs)
                
                # Create summary for this partition
                partition_summaries[partition_date] = {
                    'total_records': len(df),
                    'hours_with_data': len(hourly_aggs),
                    'total_buy_orders': hourly_aggs['buy_count'].sum(),
                    'total_sell_orders': hourly_aggs['sell_count'].sum(),
                    'avg_market_spread': hourly_aggs['market_spread'].mean(),
                    'max_buy_price': hourly_aggs['buy_max_price'].max(),
                    'min_sell_price': hourly_aggs['sell_min_price'].min()
                }
                
                logger.info(f"Partition {partition_date}: {len(hourly_aggs)} hours processed")
            else:
                logger.warning(f"No hourly aggregations generated for {partition_date}")
        
        if not all_hourly_aggs:
            raise ValueError("No hourly aggregations were generated")
        
        # Combine all hourly aggregations
        combined_hourly_aggs = pd.concat(all_hourly_aggs, ignore_index=True)
        
        # Sort by hour for better readability
        combined_hourly_aggs = combined_hourly_aggs.sort_values('hour').reset_index(drop=True)
        
        # Save hourly aggregations
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        hourly_aggs_path = f"{OUTPUT_DIR}/hourly_aggregations.csv"
        combined_hourly_aggs.to_csv(hourly_aggs_path, index=False)
        
        logger.info(f"Hourly aggregations saved to: {hourly_aggs_path}")
        
        # Create aggregation summary
        aggregation_summary = {
            'total_hours_processed': len(combined_hourly_aggs),
            'date_range': {
                'start': combined_hourly_aggs['hour'].min().isoformat(),
                'end': combined_hourly_aggs['hour'].max().isoformat()
            },
            'partition_summaries': partition_summaries,
            'overall_metrics': {
                'total_buy_orders': int(combined_hourly_aggs['buy_count'].sum()),
                'total_sell_orders': int(combined_hourly_aggs['sell_count'].sum()),
                'avg_market_spread': float(combined_hourly_aggs['market_spread'].mean()),
                'max_buy_vwap': float(combined_hourly_aggs['buy_vwap'].max()),
                'min_sell_vwap': float(combined_hourly_aggs['sell_vwap'].min())
            },
            'processing_timestamp': pd.Timestamp.now().isoformat()
        }
        
        # Save summary
        summary_path = f"{OUTPUT_DIR}/hourly_aggregations_summary.json"
        with open(summary_path, 'w') as f:
            json.dump(aggregation_summary, f, indent=2, default=str)
        
        logger.info(f"Aggregation summary saved to: {summary_path}")
        
        # Log results
        logger.info("=== HOURLY AGGREGATIONS SUMMARY ===")
        logger.info(f"Total hours processed: {len(combined_hourly_aggs)}")
        logger.info(f"Total buy orders: {aggregation_summary['overall_metrics']['total_buy_orders']}")
        logger.info(f"Total sell orders: {aggregation_summary['overall_metrics']['total_sell_orders']}")
        logger.info(f"Average market spread: €{aggregation_summary['overall_metrics']['avg_market_spread']:.2f}")
        
        # Show sample of results
        logger.info("Sample hourly aggregations:")
        sample_cols = ['hour', 'buy_count', 'sell_count', 'buy_vwap', 'sell_vwap', 'market_spread']
        available_cols = [col for col in sample_cols if col in combined_hourly_aggs.columns]
        logger.info(f"\n{combined_hourly_aggs[available_cols].head().to_string()}")
        
        logger.info("Hourly aggregations completed successfully")
        return aggregation_summary
        
    except Exception as e:
        logger.error(f"Hourly aggregations failed: {str(e)}")
        raise

def simulate_market_clearing(**context):
    """
    Task 5: Market Analysis - Market Clearing Simulation
    """
    logger.info("Running market clearing simulation...")
    
    try:
        # Import required modules
        import sys
        import os
        import json
        import pandas as pd
        
        # Import helper functions
        sys.path.append(AIRFLOW_HOME)
        sys.path.append('/opt/airflow/dags')
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        
        try:
            from utils.data_helpers import calculate_hourly_equilibrium
        except ImportError as e:
            logger.error(f"Failed to import helper functions: {e}")
            # Fallback: try importing from current directory
            import importlib.util
            spec = importlib.util.spec_from_file_location("data_helpers", "/opt/airflow/utils/data_helpers.py")
            data_helpers = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(data_helpers)
            calculate_hourly_equilibrium = data_helpers.calculate_hourly_equilibrium
        
        # Load partitioned data and process each day
        if not os.path.exists(PROCESSED_DATA_DIR):
            raise FileNotFoundError(f"Processed data directory not found: {PROCESSED_DATA_DIR}")
        
        # Find all partition directories
        partition_dirs = [d for d in os.listdir(PROCESSED_DATA_DIR) 
                         if os.path.isdir(os.path.join(PROCESSED_DATA_DIR, d))]
        
        if not partition_dirs:
            raise ValueError("No partition directories found")
        
        logger.info(f"Found {len(partition_dirs)} partitions for market clearing analysis")
        
        all_equilibrium_data = []
        partition_summaries = {}
        
        # Process each partition
        for partition_date in sorted(partition_dirs):
            partition_path = os.path.join(PROCESSED_DATA_DIR, partition_date)
            data_file = os.path.join(partition_path, 'data.parquet')
            
            if not os.path.exists(data_file):
                logger.warning(f"Data file not found for partition {partition_date}")
                continue
            
            logger.info(f"Processing market clearing for partition: {partition_date}")
            
            # Load partition data
            df = pd.read_parquet(data_file)
            logger.info(f"Loaded {len(df)} records from {partition_date}")
            
            # Calculate hourly equilibrium for this partition
            equilibrium_data = calculate_hourly_equilibrium(df, 'Timestamp')
            
            if not equilibrium_data.empty:
                # Add partition date for tracking
                equilibrium_data['date'] = partition_date
                all_equilibrium_data.append(equilibrium_data)
                
                # Create summary for this partition
                valid_equilibrium = equilibrium_data.dropna(subset=['equilibrium_price'])
                partition_summaries[partition_date] = {
                    'total_hours': len(equilibrium_data),
                    'hours_with_equilibrium': len(valid_equilibrium),
                    'avg_equilibrium_price': float(valid_equilibrium['equilibrium_price'].mean()) if not valid_equilibrium.empty else None,
                    'total_volume_traded': float(valid_equilibrium['equilibrium_volume'].sum()) if not valid_equilibrium.empty else 0,
                    'market_efficiency': float(valid_equilibrium['market_efficiency'].mean()) if not valid_equilibrium.empty else None
                }
                
                logger.info(f"Partition {partition_date}: {len(valid_equilibrium)}/{len(equilibrium_data)} hours with equilibrium")
            else:
                logger.warning(f"No equilibrium data generated for {partition_date}")
        
        if not all_equilibrium_data:
            raise ValueError("No equilibrium data was generated")
        
        # Combine all equilibrium data
        combined_equilibrium = pd.concat(all_equilibrium_data, ignore_index=True)
        
        # Sort by hour for better readability
        combined_equilibrium = combined_equilibrium.sort_values('hour').reset_index(drop=True)
        
        # Save equilibrium data
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        equilibrium_path = f"{OUTPUT_DIR}/market_equilibrium.csv"
        combined_equilibrium.to_csv(equilibrium_path, index=False)
        
        logger.info(f"Market equilibrium data saved to: {equilibrium_path}")
        
        # Create market analysis summary
        valid_equilibrium = combined_equilibrium.dropna(subset=['equilibrium_price'])
        
        market_analysis_summary = {
            'total_hours_analyzed': len(combined_equilibrium),
            'hours_with_equilibrium': len(valid_equilibrium),
            'equilibrium_success_rate': len(valid_equilibrium) / len(combined_equilibrium) * 100,
            'price_statistics': {
                'min_equilibrium_price': float(valid_equilibrium['equilibrium_price'].min()) if not valid_equilibrium.empty else None,
                'max_equilibrium_price': float(valid_equilibrium['equilibrium_price'].max()) if not valid_equilibrium.empty else None,
                'avg_equilibrium_price': float(valid_equilibrium['equilibrium_price'].mean()) if not valid_equilibrium.empty else None,
                'median_equilibrium_price': float(valid_equilibrium['equilibrium_price'].median()) if not valid_equilibrium.empty else None
            },
            'volume_statistics': {
                'total_volume_traded': float(valid_equilibrium['equilibrium_volume'].sum()) if not valid_equilibrium.empty else 0,
                'avg_hourly_volume': float(valid_equilibrium['equilibrium_volume'].mean()) if not valid_equilibrium.empty else None
            },
            'market_efficiency_metrics': {
                'avg_market_efficiency': float(valid_equilibrium['market_efficiency'].mean()) if not valid_equilibrium.empty else None,
                'avg_bid_ask_spread': float(valid_equilibrium['bid_ask_spread'].mean()) if not valid_equilibrium.empty else None
            },
            'partition_summaries': partition_summaries,
            'processing_timestamp': pd.Timestamp.now().isoformat()
        }
        
        # Save summary
        summary_path = f"{OUTPUT_DIR}/market_analysis_summary.json"
        with open(summary_path, 'w') as f:
            json.dump(market_analysis_summary, f, indent=2, default=str)
        
        logger.info(f"Market analysis summary saved to: {summary_path}")
        
        # Log results
        logger.info("=== MARKET CLEARING ANALYSIS SUMMARY ===")
        logger.info(f"Total hours analyzed: {len(combined_equilibrium)}")
        logger.info(f"Hours with equilibrium: {len(valid_equilibrium)} ({market_analysis_summary['equilibrium_success_rate']:.1f}%)")
        
        if not valid_equilibrium.empty:
            logger.info(f"Price range: €{market_analysis_summary['price_statistics']['min_equilibrium_price']:.2f} - €{market_analysis_summary['price_statistics']['max_equilibrium_price']:.2f}")
            logger.info(f"Average equilibrium price: €{market_analysis_summary['price_statistics']['avg_equilibrium_price']:.2f}")
            logger.info(f"Total volume traded: {market_analysis_summary['volume_statistics']['total_volume_traded']:.0f} MWh")
            logger.info(f"Average market efficiency: {market_analysis_summary['market_efficiency_metrics']['avg_market_efficiency']:.2%}")
        
        # Show sample of results
        logger.info("Sample equilibrium data:")
        sample_cols = ['hour', 'equilibrium_price', 'equilibrium_volume', 'market_efficiency']
        available_cols = [col for col in sample_cols if col in combined_equilibrium.columns]
        logger.info(f"\n{combined_equilibrium[available_cols].head().to_string()}")
        
        logger.info("Market clearing simulation completed successfully")
        return market_analysis_summary
        
    except Exception as e:
        logger.error(f"Market clearing simulation failed: {str(e)}")
        raise

def renewable_impact_analysis(**context):
    """
    Task 6: Renewable Energy Impact Simulation
    """
    logger.info("Starting renewable impact analysis...")
    
    try:
        # Import required modules
        import sys
        import os
        import json
        import pandas as pd
        
        # Import helper functions
        sys.path.append(AIRFLOW_HOME)
        sys.path.append('/opt/airflow/dags')
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        
        try:
            from utils.data_helpers import calculate_renewable_impact_scenarios
        except ImportError as e:
            logger.error(f"Failed to import helper functions: {e}")
            # Fallback: try importing from current directory
            import importlib.util
            spec = importlib.util.spec_from_file_location("data_helpers", "/opt/airflow/utils/data_helpers.py")
            data_helpers = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(data_helpers)
            calculate_renewable_impact_scenarios = data_helpers.calculate_renewable_impact_scenarios
        
        # Load partitioned data and process each day
        if not os.path.exists(PROCESSED_DATA_DIR):
            raise FileNotFoundError(f"Processed data directory not found: {PROCESSED_DATA_DIR}")
        
        # Find all partition directories
        partition_dirs = [d for d in os.listdir(PROCESSED_DATA_DIR) 
                         if os.path.isdir(os.path.join(PROCESSED_DATA_DIR, d))]
        
        if not partition_dirs:
            raise ValueError("No partition directories found")
        
        logger.info(f"Found {len(partition_dirs)} partitions for renewable impact analysis")
        
        # Define scenarios
        HIGH_RENEWABLE_SHIFT = 2000  # +2000 MWh additional renewable energy
        LOW_RENEWABLE_SHIFT = -2000  # -2000 MWh renewable energy shortage
        
        logger.info(f"Scenario A (High Renewables): +{HIGH_RENEWABLE_SHIFT} MWh")
        logger.info(f"Scenario B (Low Renewables): {LOW_RENEWABLE_SHIFT} MWh")
        
        all_scenario_data = []
        partition_summaries = {}
        
        # Process each partition
        for partition_date in sorted(partition_dirs):
            partition_path = os.path.join(PROCESSED_DATA_DIR, partition_date)
            data_file = os.path.join(partition_path, 'data.parquet')
            
            if not os.path.exists(data_file):
                logger.warning(f"Data file not found for partition {partition_date}")
                continue
            
            logger.info(f"Processing renewable scenarios for partition: {partition_date}")
            
            # Load partition data
            df = pd.read_parquet(data_file)
            logger.info(f"Loaded {len(df)} records from {partition_date}")
            
            # Calculate renewable impact scenarios for this partition
            scenario_data = calculate_renewable_impact_scenarios(
                df, 'Timestamp', HIGH_RENEWABLE_SHIFT, LOW_RENEWABLE_SHIFT
            )
            
            if not scenario_data.empty:
                # Add partition date for tracking
                scenario_data['date'] = partition_date
                all_scenario_data.append(scenario_data)
                
                # Create summary for this partition
                valid_scenarios = scenario_data.dropna(subset=['baseline_price'])
                
                if not valid_scenarios.empty:
                    partition_summaries[partition_date] = {
                        'total_hours': len(scenario_data),
                        'hours_with_valid_scenarios': len(valid_scenarios),
                        'avg_baseline_price': float(valid_scenarios['baseline_price'].mean()),
                        'avg_high_renewable_price': float(valid_scenarios['high_renewable_price'].mean()),
                        'avg_low_renewable_price': float(valid_scenarios['low_renewable_price'].mean()),
                        'avg_high_renewable_impact': float(valid_scenarios['high_renewable_price_impact'].mean()),
                        'avg_low_renewable_impact': float(valid_scenarios['low_renewable_price_impact'].mean()),
                        'max_price_reduction': float(valid_scenarios['high_renewable_price_impact'].min()),
                        'max_price_increase': float(valid_scenarios['low_renewable_price_impact'].max())
                    }
                
                logger.info(f"Partition {partition_date}: {len(valid_scenarios)}/{len(scenario_data)} hours with valid scenarios")
            else:
                logger.warning(f"No scenario data generated for {partition_date}")
        
        if not all_scenario_data:
            raise ValueError("No renewable scenario data was generated")
        
        # Combine all scenario data
        combined_scenarios = pd.concat(all_scenario_data, ignore_index=True)
        
        # Sort by hour for better readability
        combined_scenarios = combined_scenarios.sort_values('hour').reset_index(drop=True)
        
        # Save scenario data
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        scenarios_path = f"{OUTPUT_DIR}/renewable_impact_scenarios.csv"
        combined_scenarios.to_csv(scenarios_path, index=False)
        
        logger.info(f"Renewable impact scenarios saved to: {scenarios_path}")
        
        # Create renewable impact analysis summary
        valid_scenarios = combined_scenarios.dropna(subset=['baseline_price'])
        
        if not valid_scenarios.empty:
            renewable_analysis_summary = {
                'total_hours_analyzed': len(combined_scenarios),
                'hours_with_valid_scenarios': len(valid_scenarios),
                'scenario_success_rate': len(valid_scenarios) / len(combined_scenarios) * 100,
                'baseline_statistics': {
                    'avg_price': float(valid_scenarios['baseline_price'].mean()),
                    'min_price': float(valid_scenarios['baseline_price'].min()),
                    'max_price': float(valid_scenarios['baseline_price'].max())
                },
                'high_renewable_impact': {
                    'avg_price': float(valid_scenarios['high_renewable_price'].mean()),
                    'avg_price_impact': float(valid_scenarios['high_renewable_price_impact'].mean()),
                    'avg_price_change_pct': float(valid_scenarios['high_renewable_price_change_pct'].mean()),
                    'max_price_reduction': float(valid_scenarios['high_renewable_price_impact'].min()),
                    'max_price_reduction_pct': float(valid_scenarios['high_renewable_price_change_pct'].min())
                },
                'low_renewable_impact': {
                    'avg_price': float(valid_scenarios['low_renewable_price'].mean()),
                    'avg_price_impact': float(valid_scenarios['low_renewable_price_impact'].mean()),
                    'avg_price_change_pct': float(valid_scenarios['low_renewable_price_change_pct'].mean()),
                    'max_price_increase': float(valid_scenarios['low_renewable_price_impact'].max()),
                    'max_price_increase_pct': float(valid_scenarios['low_renewable_price_change_pct'].max())
                },
                'policy_insights': {
                    'renewable_benefit_eur_per_mwh': abs(float(valid_scenarios['high_renewable_price_impact'].mean())),
                    'renewable_shortage_cost_eur_per_mwh': float(valid_scenarios['low_renewable_price_impact'].mean()),
                    'price_volatility_reduction': float(valid_scenarios['high_renewable_price'].std()) < float(valid_scenarios['baseline_price'].std())
                },
                'partition_summaries': partition_summaries,
                'processing_timestamp': pd.Timestamp.now().isoformat()
            }
        else:
            renewable_analysis_summary = {
                'error': 'No valid scenarios generated',
                'total_hours_analyzed': len(combined_scenarios)
            }
        
        # Save summary
        summary_path = f"{OUTPUT_DIR}/renewable_impact_summary.json"
        with open(summary_path, 'w') as f:
            json.dump(renewable_analysis_summary, f, indent=2, default=str)
        
        logger.info(f"Renewable impact summary saved to: {summary_path}")
        
        # Log results
        if valid_scenarios is not None and not valid_scenarios.empty:
            logger.info("=== RENEWABLE ENERGY IMPACT ANALYSIS ===")
            logger.info(f"Total hours analyzed: {len(combined_scenarios)}")
            logger.info(f"Hours with valid scenarios: {len(valid_scenarios)}")
            logger.info(f"Baseline average price: €{renewable_analysis_summary['baseline_statistics']['avg_price']:.2f}/MWh")
            
            logger.info(f"\n🌱 HIGH RENEWABLE SCENARIO (+{HIGH_RENEWABLE_SHIFT} MWh):")
            logger.info(f"  Average price: €{renewable_analysis_summary['high_renewable_impact']['avg_price']:.2f}/MWh")
            logger.info(f"  Price impact: €{renewable_analysis_summary['high_renewable_impact']['avg_price_impact']:.2f}/MWh ({renewable_analysis_summary['high_renewable_impact']['avg_price_change_pct']:.1f}%)")
            logger.info(f"  Max price reduction: €{renewable_analysis_summary['high_renewable_impact']['max_price_reduction']:.2f}/MWh")
            
            logger.info(f"\n⚡ LOW RENEWABLE SCENARIO ({LOW_RENEWABLE_SHIFT} MWh):")
            logger.info(f"  Average price: €{renewable_analysis_summary['low_renewable_impact']['avg_price']:.2f}/MWh")
            logger.info(f"  Price impact: €{renewable_analysis_summary['low_renewable_impact']['avg_price_impact']:.2f}/MWh ({renewable_analysis_summary['low_renewable_impact']['avg_price_change_pct']:.1f}%)")
            logger.info(f"  Max price increase: €{renewable_analysis_summary['low_renewable_impact']['max_price_increase']:.2f}/MWh")
            
            logger.info(f"\n📊 POLICY INSIGHTS:")
            logger.info(f"  Renewable benefit: €{renewable_analysis_summary['policy_insights']['renewable_benefit_eur_per_mwh']:.2f}/MWh saved")
            logger.info(f"  Shortage cost: €{renewable_analysis_summary['policy_insights']['renewable_shortage_cost_eur_per_mwh']:.2f}/MWh penalty")
        
        logger.info("Renewable impact analysis completed successfully")
        return renewable_analysis_summary
        
    except Exception as e:
        logger.error(f"Renewable impact analysis failed: {str(e)}")
        raise

def generate_final_summary(**context):
    """
    Task 7: Output Generation
    """
    logger.info("Generating final summary...")
    
    try:
        # Import required modules
        import os
        import json
        import pandas as pd
        
        # Check if all required input files exist
        required_files = {
            'hourly_aggregations': f"{OUTPUT_DIR}/hourly_aggregations.csv",
            'market_equilibrium': f"{OUTPUT_DIR}/market_equilibrium.csv", 
            'renewable_scenarios': f"{OUTPUT_DIR}/renewable_impact_scenarios.csv"
        }
        
        missing_files = []
        for name, path in required_files.items():
            if not os.path.exists(path):
                missing_files.append(f"{name}: {path}")
        
        if missing_files:
            raise FileNotFoundError(f"Missing required input files: {missing_files}")
        
        logger.info("Loading analysis results for final summary...")
        
        # Load all analysis results
        hourly_aggs = pd.read_csv(required_files['hourly_aggregations'])
        market_equilibrium = pd.read_csv(required_files['market_equilibrium'])
        renewable_scenarios = pd.read_csv(required_files['renewable_scenarios'])
        
        logger.info(f"Loaded data: {len(hourly_aggs)} hourly aggs, {len(market_equilibrium)} equilibrium points, {len(renewable_scenarios)} scenarios")
        
        # Ensure all dataframes have datetime hour columns
        for df, name in [(hourly_aggs, 'hourly_aggs'), (market_equilibrium, 'market_equilibrium'), (renewable_scenarios, 'renewable_scenarios')]:
            if 'hour' in df.columns:
                df['hour'] = pd.to_datetime(df['hour'])
                logger.info(f"Converted {name} hour column to datetime")
        
        # Start with hourly aggregations as the base
        final_summary = hourly_aggs.copy()
        
        # Merge with market equilibrium data
        equilibrium_cols = ['hour', 'equilibrium_price', 'equilibrium_volume']
        available_eq_cols = [col for col in equilibrium_cols if col in market_equilibrium.columns]
        
        if available_eq_cols:
            final_summary = final_summary.merge(
                market_equilibrium[available_eq_cols], 
                on='hour', 
                how='left'
            )
            logger.info(f"Merged equilibrium data: {len(available_eq_cols)} columns")
        else:
            logger.warning("No equilibrium columns found to merge")
        
        # Merge with renewable scenario data
        scenario_cols = ['hour', 'high_renewable_price', 'low_renewable_price']
        available_scenario_cols = [col for col in scenario_cols if col in renewable_scenarios.columns]
        
        if available_scenario_cols:
            final_summary = final_summary.merge(
                renewable_scenarios[available_scenario_cols],
                on='hour',
                how='left'
            )
            logger.info(f"Merged renewable scenario data: {len(available_scenario_cols)} columns")
        else:
            logger.warning("No renewable scenario columns found to merge")
        
        # Rename columns to match the required output format
        column_mapping = {
            'high_renewable_price': 'equilibrium_price_high_renewables',
            'low_renewable_price': 'equilibrium_price_low_renewables'
        }
        
        final_summary = final_summary.rename(columns=column_mapping)
        
        # Ensure all required columns are present (add missing ones with NaN)
        required_columns = [
            'hour', 'buy_count', 'sell_count', 'buy_avg_price', 'sell_avg_price',
            'buy_total_volume', 'sell_total_volume', 'buy_vwap', 'sell_vwap',
            'market_spread', 'equilibrium_price', 
            'equilibrium_price_high_renewables', 'equilibrium_price_low_renewables'
        ]
        
        for col in required_columns:
            if col not in final_summary.columns:
                final_summary[col] = np.nan
                logger.warning(f"Added missing column '{col}' with NaN values")
        
        # Reorder columns to match specification
        final_summary = final_summary[required_columns]
        
        # Sort by hour for better readability
        final_summary = final_summary.sort_values('hour').reset_index(drop=True)
        
        # Clean up data types and formatting
        # Round numerical columns to reasonable precision
        numerical_columns = [col for col in final_summary.columns if col != 'hour']
        for col in numerical_columns:
            if final_summary[col].dtype in ['float64', 'int64']:
                final_summary[col] = final_summary[col].round(2)
        
        # Save final summary
        final_summary_path = f"{OUTPUT_DIR}/final_summary.csv"
        final_summary.to_csv(final_summary_path, index=False)
        
        logger.info(f"Final summary saved to: {final_summary_path}")
        
        # Create summary statistics
        valid_data = final_summary.dropna(subset=['equilibrium_price'])
        
        summary_stats = {
            'total_hours': len(final_summary),
            'hours_with_complete_data': len(valid_data),
            'data_completeness_rate': len(valid_data) / len(final_summary) * 100,
            'date_range': {
                'start': str(final_summary['hour'].min()),
                'end': str(final_summary['hour'].max())
            },
            'market_statistics': {},
            'renewable_impact_statistics': {},
            'data_quality_metrics': {}
        }
        
        if not valid_data.empty:
            # Market statistics
            summary_stats['market_statistics'] = {
                'avg_equilibrium_price': float(valid_data['equilibrium_price'].mean()),
                'min_equilibrium_price': float(valid_data['equilibrium_price'].min()),
                'max_equilibrium_price': float(valid_data['equilibrium_price'].max()),
                'avg_market_spread': float(valid_data['market_spread'].mean()),
                'total_buy_orders': int(valid_data['buy_count'].sum()),
                'total_sell_orders': int(valid_data['sell_count'].sum()),
                'total_volume_traded': float(valid_data['buy_total_volume'].sum() + valid_data['sell_total_volume'].sum())
            }
            
            # Renewable impact statistics
            renewable_valid = valid_data.dropna(subset=['equilibrium_price_high_renewables', 'equilibrium_price_low_renewables'])
            if not renewable_valid.empty:
                high_renewable_impact = renewable_valid['equilibrium_price_high_renewables'] - renewable_valid['equilibrium_price']
                low_renewable_impact = renewable_valid['equilibrium_price_low_renewables'] - renewable_valid['equilibrium_price']
                
                summary_stats['renewable_impact_statistics'] = {
                    'avg_high_renewable_price_impact': float(high_renewable_impact.mean()),
                    'avg_low_renewable_price_impact': float(low_renewable_impact.mean()),
                    'max_price_reduction_high_renewable': float(high_renewable_impact.min()),
                    'max_price_increase_low_renewable': float(low_renewable_impact.max()),
                    'renewable_benefit_eur_per_mwh': float(abs(high_renewable_impact.mean())),
                    'renewable_shortage_cost_eur_per_mwh': float(low_renewable_impact.mean())
                }
            
            # Data quality metrics
            summary_stats['data_quality_metrics'] = {
                'missing_equilibrium_prices': int(final_summary['equilibrium_price'].isnull().sum()),
                'missing_renewable_scenarios': int(final_summary['equilibrium_price_high_renewables'].isnull().sum()),
                'complete_records': int((~final_summary[required_columns].isnull().any(axis=1)).sum()),
                'avg_buy_vwap': float(valid_data['buy_vwap'].mean()),
                'avg_sell_vwap': float(valid_data['sell_vwap'].mean())
            }
        
        # Save summary statistics
        summary_stats['processing_timestamp'] = pd.Timestamp.now().isoformat()
        summary_stats_path = f"{OUTPUT_DIR}/final_summary_statistics.json"
        
        with open(summary_stats_path, 'w') as f:
            json.dump(summary_stats, f, indent=2, default=str)
        
        logger.info(f"Summary statistics saved to: {summary_stats_path}")
        
        # Log results
        logger.info("=== FINAL SUMMARY GENERATION COMPLETE ===")
        logger.info(f"Total hours in summary: {len(final_summary)}")
        logger.info(f"Hours with complete data: {len(valid_data)} ({summary_stats['data_completeness_rate']:.1f}%)")
        logger.info(f"Date range: {summary_stats['date_range']['start']} to {summary_stats['date_range']['end']}")
        
        if summary_stats['market_statistics']:
            logger.info(f"Average equilibrium price: €{summary_stats['market_statistics']['avg_equilibrium_price']:.2f}/MWh")
            logger.info(f"Average market spread: €{summary_stats['market_statistics']['avg_market_spread']:.2f}/MWh")
            logger.info(f"Total orders processed: {summary_stats['market_statistics']['total_buy_orders']} buy, {summary_stats['market_statistics']['total_sell_orders']} sell")
        
        if summary_stats['renewable_impact_statistics']:
            logger.info(f"Renewable energy benefit: €{summary_stats['renewable_impact_statistics']['renewable_benefit_eur_per_mwh']:.2f}/MWh")
            logger.info(f"Renewable shortage cost: €{summary_stats['renewable_impact_statistics']['renewable_shortage_cost_eur_per_mwh']:.2f}/MWh")
        
        # Show sample of final output
        logger.info("Sample of final summary (first 5 rows):")
        sample_cols = ['hour', 'buy_count', 'sell_count', 'equilibrium_price', 'equilibrium_price_high_renewables', 'equilibrium_price_low_renewables']
        available_sample_cols = [col for col in sample_cols if col in final_summary.columns]
        logger.info(f"\n{final_summary[available_sample_cols].head().to_string()}")
        
        logger.info("Final summary generation completed successfully")
        return summary_stats
        
    except Exception as e:
        logger.error(f"Final summary generation failed: {str(e)}")
        raise

def setup_monitoring(**context):
    """
    Task 8: Pipeline Monitoring & Alerting
    """
    logger.info("Setting up monitoring and alerts...")
    
    try:
        # Import required modules
        import os
        import json
        import pandas as pd
        import sys
        
        # Import helper functions
        sys.path.append(AIRFLOW_HOME)
        sys.path.append('/opt/airflow/dags')
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        
        try:
            from utils.data_helpers import validate_pipeline_health, detect_timestamp_gaps
        except ImportError as e:
            logger.error(f"Failed to import helper functions: {e}")
            # Fallback: try importing from current directory
            import importlib.util
            spec = importlib.util.spec_from_file_location("data_helpers", "/opt/airflow/utils/data_helpers.py")
            data_helpers = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(data_helpers)
            validate_pipeline_health = data_helpers.validate_pipeline_health
            detect_timestamp_gaps = data_helpers.detect_timestamp_gaps
        import sys
        import os
        sys.path.append(AIRFLOW_HOME)
        sys.path.append('/opt/airflow/dags')
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        
        from utils.data_helpers import detect_timestamp_gaps, validate_pipeline_health
        
        # Collect results from previous tasks using XCom
        ti = context['task_instance']
        
        # Get results from previous tasks
        validation_results = ti.xcom_pull(task_ids='validate_data_quality')
        aggregation_results = ti.xcom_pull(task_ids='calculate_hourly_aggregations')
        equilibrium_results = ti.xcom_pull(task_ids='simulate_market_clearing')
        renewable_results = ti.xcom_pull(task_ids='renewable_impact_analysis')
        summary_results = ti.xcom_pull(task_ids='generate_final_summary')
        
        logger.info("Retrieved results from previous tasks for monitoring analysis")
        
        # Load the cleaned data for timestamp gap analysis
        cleaned_data_path = f"{OUTPUT_DIR}/cleaned_data.csv"
        timestamp_gap_analysis = {}
        
        if os.path.exists(cleaned_data_path):
            logger.info("Performing timestamp gap analysis...")
            df = pd.read_csv(cleaned_data_path)
            df['Timestamp'] = pd.to_datetime(df['Timestamp'])
            
            # Detect timestamp gaps (threshold: 2 hours for energy data)
            timestamp_gap_analysis = detect_timestamp_gaps(df, 'Timestamp', max_gap_minutes=120)
            
            if timestamp_gap_analysis['gaps_detected']:
                logger.warning(f"Timestamp gaps detected: {timestamp_gap_analysis['total_gaps']} gaps")
                logger.warning(f"Data continuity score: {timestamp_gap_analysis['data_continuity_score']:.1f}%")
            else:
                logger.info("No significant timestamp gaps detected")
        else:
            logger.warning(f"Cleaned data file not found for timestamp analysis: {cleaned_data_path}")
        
        # Perform pipeline health check
        logger.info("Performing pipeline health assessment...")
        
        pipeline_health = validate_pipeline_health(
            validation_results, aggregation_results, 
            equilibrium_results, renewable_results
        )
        
        # Add timestamp gap analysis to health check
        pipeline_health['timestamp_gap_analysis'] = timestamp_gap_analysis
        
        # Define critical failure conditions
        critical_failures = []
        
        # Check data quality thresholds
        if validation_results and not validation_results.get('validation_passed', True):
            critical_failures.append("Data validation failed - pipeline should not proceed")
        
        # Check for excessive missing data
        if validation_results:
            missing_percentages = validation_results.get('missing_percentages', {})
            for col, pct in missing_percentages.items():
                if col in ['Price', 'Volume'] and pct > 10:
                    critical_failures.append(f"Critical field '{col}' has {pct}% missing data (threshold: 10%)")
        
        # Check timestamp gaps
        if timestamp_gap_analysis.get('data_continuity_score', 100) < 80:
            critical_failures.append(f"Poor data continuity: {timestamp_gap_analysis['data_continuity_score']:.1f}% (threshold: 80%)")
        
        # Check pipeline execution success
        failed_tasks = [task for task, status in pipeline_health['task_status'].items() if status == 'FAILED']
        if failed_tasks:
            critical_failures.append(f"Critical tasks failed: {failed_tasks}")
        
        # Determine overall pipeline status
        pipeline_status = "HEALTHY" if not critical_failures else "CRITICAL"
        
        # Create monitoring report
        monitoring_report = {
            'pipeline_status': pipeline_status,
            'monitoring_timestamp': pd.Timestamp.now().isoformat(),
            'execution_date': str(context.get('execution_date', 'Unknown')),
            'dag_run_id': str(getattr(context.get('dag_run'), 'run_id', 'Unknown')),
            'pipeline_health': pipeline_health,
            'critical_failures': critical_failures,
            'data_quality_summary': {
                'validation_passed': validation_results.get('validation_passed', False) if validation_results else False,
                'total_records_processed': validation_results.get('total_records', 0) if validation_results else 0,
                'missing_data_issues': len([col for col, pct in validation_results.get('missing_percentages', {}).items() if pct > 5]) if validation_results else 0,
                'business_logic_issues': len(validation_results.get('business_logic_issues', [])) if validation_results else 0
            },
            'performance_summary': {
                'hours_processed': aggregation_results.get('total_hours_processed', 0) if aggregation_results else 0,
                'equilibrium_success_rate': equilibrium_results.get('equilibrium_success_rate', 0) if equilibrium_results else 0,
                'renewable_scenarios_analyzed': renewable_results.get('total_hours_analyzed', 0) if renewable_results else 0,
                'final_summary_completeness': summary_results.get('data_completeness_rate', 0) if summary_results else 0
            },
            'alerting_configuration': {
                'email_alerts_enabled': False,  # Would be configured in production
                'smtp_configured': False,
                'alert_recipients': [],  # Would be configured in production
                'alert_thresholds': {
                    'max_missing_data_pct': 10.0,
                    'min_data_continuity_pct': 80.0,
                    'min_equilibrium_success_rate': 50.0,
                    'min_pipeline_health_score': 70.0
                }
            }
        }
        
        # Save monitoring report
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        monitoring_report_path = f"{OUTPUT_DIR}/pipeline_monitoring_report.json"
        
        with open(monitoring_report_path, 'w') as f:
            json.dump(monitoring_report, f, indent=2, default=str)
        
        logger.info(f"Pipeline monitoring report saved to: {monitoring_report_path}")
        
        # Log monitoring results
        logger.info("=== PIPELINE MONITORING SUMMARY ===")
        logger.info(f"Pipeline Status: {pipeline_status}")
        logger.info(f"Health Score: {pipeline_health['health_score']:.1f}%")
        logger.info(f"Critical Issues: {len(critical_failures)}")
        logger.info(f"Warnings: {len(pipeline_health['warnings'])}")
        
        if critical_failures:
            logger.error("CRITICAL FAILURES DETECTED:")
            for failure in critical_failures:
                logger.error(f"  - {failure}")
        
        if pipeline_health['warnings']:
            logger.warning("WARNINGS:")
            for warning in pipeline_health['warnings']:
                logger.warning(f"  - {warning}")
        
        # Performance summary
        logger.info(f"Records processed: {monitoring_report['data_quality_summary']['total_records_processed']}")
        logger.info(f"Hours analyzed: {monitoring_report['performance_summary']['hours_processed']}")
        logger.info(f"Equilibrium success rate: {monitoring_report['performance_summary']['equilibrium_success_rate']:.1f}%")
        
        # Timestamp gap summary
        if timestamp_gap_analysis:
            logger.info(f"Data continuity: {timestamp_gap_analysis['data_continuity_score']:.1f}%")
            if timestamp_gap_analysis['gaps_detected']:
                logger.info(f"Timestamp gaps: {timestamp_gap_analysis['total_gaps']} detected")
        
        # Email alerting simulation (would be real SMTP in production)
        if critical_failures:
            alert_message = f"""
CRITICAL ALERT: Energy Data Pipeline Failure

Pipeline Status: {pipeline_status}
Execution Date: {context.get('execution_date', 'Unknown')}
DAG Run ID: {getattr(context.get('dag_run'), 'run_id', 'Unknown')}

Critical Failures:
{chr(10).join(f'- {failure}' for failure in critical_failures)}

Health Score: {pipeline_health['health_score']:.1f}%

Please investigate immediately.
            """
            
            logger.error("EMAIL ALERT WOULD BE SENT:")
            logger.error(alert_message)
            
            # In production, you would send actual email here:
            # send_email(
            #     to=['data-team@company.com'],
            #     subject='CRITICAL: Energy Data Pipeline Failure',
            #     html_content=alert_message
            # )
        
        # Fail the task if critical issues are detected
        if critical_failures:
            raise ValueError(f"Pipeline monitoring detected critical failures: {critical_failures}")
        
        logger.info("Pipeline monitoring and alerting setup completed successfully")
        return monitoring_report
        
    except Exception as e:
        logger.error(f"Pipeline monitoring setup failed: {str(e)}")
        
        # Create emergency monitoring report
        emergency_report = {
            'pipeline_status': 'MONITORING_FAILED',
            'monitoring_timestamp': pd.Timestamp.now().isoformat(),
            'error': str(e),
            'critical_failures': ['Monitoring system failure']
        }
        
        # Save emergency report
        try:
            emergency_path = f"{OUTPUT_DIR}/emergency_monitoring_report.json"
            with open(emergency_path, 'w') as f:
                json.dump(emergency_report, f, indent=2, default=str)
            logger.error(f"Emergency monitoring report saved to: {emergency_path}")
        except:
            logger.error("Failed to save emergency monitoring report")
        
        raise

# Test environment
test_env = PythonOperator(
    task_id='test_environment',
    python_callable=test_environment,
    dag=dag,
)

# Create directory structure
create_directories = BashOperator(
    task_id='create_directories',
    bash_command=f'mkdir -p {PROCESSED_DATA_DIR} {OUTPUT_DIR}',
    dag=dag,
)

# Task 1: Data Quality Validation
validate_quality = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    dag=dag,
)

# Task 2: Data Cleaning
clean_data = PythonOperator(
    task_id='clean_and_standardize_data', 
    python_callable=clean_and_standardize_data,
    dag=dag,
)

# Task 3: Data Partitioning
partition_data = PythonOperator(
    task_id='partition_and_store_data',
    python_callable=partition_and_store_data,
    dag=dag,
)

# Task 4: Hourly Aggregations
hourly_aggs = PythonOperator(
    task_id='calculate_hourly_aggregations',
    python_callable=calculate_hourly_aggregations,
    dag=dag,
)

# Task 5: Market Clearing
market_clearing = PythonOperator(
    task_id='simulate_market_clearing',
    python_callable=simulate_market_clearing,
    dag=dag,
)

# Task 6: Renewable Impact
renewable_analysis = PythonOperator(
    task_id='renewable_impact_analysis',
    python_callable=renewable_impact_analysis,
    dag=dag,
)

# Task 7: Final Summary
final_summary = PythonOperator(
    task_id='generate_final_summary',
    python_callable=generate_final_summary,
    dag=dag,
)

# Task 8: Monitoring Setup
monitoring = PythonOperator(
    task_id='setup_monitoring',
    python_callable=setup_monitoring,
    dag=dag,
)

# Define task dependencies
test_env >> create_directories >> validate_quality >> clean_data >> partition_data
partition_data >> hourly_aggs >> market_clearing >> renewable_analysis
renewable_analysis >> final_summary
final_summary >> monitoring
