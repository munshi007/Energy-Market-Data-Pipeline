# Student Data Engineer Challenge

## Overview

Welcome to Terra One's Student Data Engineer coding challenge! This challenge is designed to assess your data engineering skills through building a real data pipeline with workflow orchestration - the kind of work you'll do daily at Terra One.

As a Student Data Engineer, you'll support our mission of accelerating renewable energy adoption by building robust data pipelines that power our AI-driven energy trading platform. This challenge reflects actual tasks you'll encounter working with energy market data.

## The Challenge

You'll build an **Airflow-based data pipeline** that processes energy market bid data, demonstrating key data engineering concepts including:
- Workflow orchestration with Apache Airflow
- Data quality validation and cleaning
- Batch processing and partitioning  
- Data aggregation and analytics
- Error handling and monitoring

We understand that you may use tools like ChatGPT or other LLMs to assist in solving this case study, which is perfectly fine, but we encourage you to first make your own assumptions and ensure that you fully understand the concepts you're defining when presenting your solutions.

## Data Overview

The provided CSV file `dags/energy_data.csv` contains 3 days of energy market bid data (~3000 records) with these fields:

- **Timestamp**
- **Price**: Bid price in €/MWh
- **Volume**: Bid volume in MWh 
- **Sell_Buy**: Whether this is a sell or buy order

The dataset intentionally contains realistic data quality issues you'll encounter in production systems.

## Setup Instructions

### Prerequisites
- **Docker Desktop** installed and running
- **Python 3.8+**
- **Git** for version control
- At least **4GB RAM** available for Docker

### Quick Start

1. **Clone and setup the repository:**
```bash
git clone <this-repo>
cd t1-coding-challenge-student-data-engineer
```

2. **Start Airflow with Docker Compose:**
```bash
# Initialize Airflow database and create admin user
docker compose up airflow-init

# Start all services (webserver, scheduler, database)
docker compose up -d

# Check if services are running
docker compose ps
```

3. **Access Airflow Web UI:**
- Open http://localhost:8080
- Login credentials:
  - **Username**: `airflow`
  - **Password**: `airflow`

4. **Verify Setup:**
- In the Airflow UI, you should see the `energy_data_pipeline` DAG
- The DAG should be paused by default (toggle to unpause when ready to test)
- Check that all required directories are created

5. **Development Setup (Optional):**
If you want to run Python scripts locally for development:
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Your Tasks

### Part 1: Data Pipeline Development (2 hours)

Create an Airflow DAG called `energy_data_pipeline` that implements these tasks:

#### Task 1: Data Validation & Quality Check
- Load and validate the dataset
- Generate a comprehensive data quality report

#### Task 2: Data Cleaning & Standardization
- Clean and standardize the data to ensure consistency
- How would you handle missing values and outliers appropriately?
- Ensure data integrity for downstream processing

#### Task 3: Data Partitioning & Storage
- Partition cleaned data by day
- Save each day's data as Parquet files in `processed_data/YYYY-MM-DD/` 
- Include data lineage information (original row count, cleaning steps applied)

#### Task 4: Hourly Aggregations
For each day's data, calculate hourly metrics:
- **Buy orders**: count, avg price, total volume, min/max price
- **Sell orders**: count, avg price, total volume, min/max price  
- **Volume-Weighted Average Price (VWAP)** for both buy and sell:
  ```
  VWAP = Σ(Price × Volume) / Σ(Volume)
  ```
- **Market spread**: difference between avg buy and sell prices

#### Task 5: Market Analysis
Implement a simplified market clearing simulation:
- Sort sell orders by price (ascending) = supply curve
- Sort buy orders by price (descending) = demand curve  
- Find intersection point where cumulative buy volume meets sell volume
- Calculate equilibrium price for each hour

### Part 2: Advanced Analytics (1 hour)

#### Task 6: Renewable Energy Impact Simulation
Simulate the impact of additional renewable generation:
- **Scenario A**: +2000 MWh renewable energy (shifts supply curve)
- **Scenario B**: -2000 MWh renewable energy shortage
- Recalculate equilibrium prices under both scenarios
- Compare price impacts vs baseline

#### Task 7: Output Generation
Create a final summary CSV with columns:
- `hour`, `buy_count`, `sell_count`, `buy_avg_price`, `sell_avg_price`
- `buy_total_volume`, `sell_total_volume`, `buy_vwap`, `sell_vwap`
- `market_spread`, `equilibrium_price`, `equilibrium_price_high_renewables`, `equilibrium_price_low_renewables`

### Part 3: Pipeline Monitoring (30 mins)

#### Task 8: Add Monitoring & Alerting
- Implement data quality checks that fail the pipeline if:
  - >10% of data is missing critical fields
  - Price outliers exceed reasonable thresholds  
  - Timestamp gaps are too large
- Add logging throughout the pipeline
- Use Airflow's email alerts for failures (configure SMTP or log-only)

## Technical Requirements

### Required Technologies:
- **Apache Airflow** for workflow orchestration
- **pandas** for data manipulation  
- **pyarrow** for Parquet file handling
- **Docker** for easy setup

### Pipeline Architecture:
- All tasks should be idempotent (can be re-run safely)
- Use appropriate Airflow operators (PythonOperator, BashOperator, etc.)
- Implement proper task dependencies  
- Include retry logic for transient failures

### Code Quality:
- Clean, documented Python functions
- Proper error handling and logging
- Configuration externalized (no hardcoded paths)
- Follow PEP 8 style guidelines

## Deliverables

1. **Airflow DAG file** (`dags/energy_data_pipeline.py`)
2. **Supporting Python modules** (utilities, data processing functions)
3. **Docker Compose configuration** (provided as starter)
4. **Output data files**:
   - Daily Parquet files in `processed_data/`
   - Final summary CSV
   - Data quality reports
5. **Documentation** explaining:
   - Your approach to data quality issues
   - Design decisions and trade-offs
   - How to run and monitor the pipeline

## Evaluation Criteria

**Technical Implementation (60%)**:
- Correct Airflow DAG structure and dependencies
- Robust data cleaning and validation logic
- Proper error handling and edge case management
- Code organization and documentation

**Data Engineering Best Practices (25%)**:  
- Idempotent pipeline design
- Appropriate use of Airflow features
- Data partitioning and storage strategy
- Monitoring and alerting implementation

**Problem Solving & Analysis (15%)**:
- Thoughtful approach to data quality issues  
- Reasonable assumptions and trade-offs
- Clear explanation of methodology

## Time Estimate

**Total: 3.5 hours**
- Setup and exploration: 30 mins
- Core pipeline development: 2 hours  
- Advanced analytics: 1 hour
- Documentation and testing: 30 mins

## Tips for Success

1. **Start simple** - get basic tasks working before adding complexity
2. **Test incrementally** - run individual tasks before the full pipeline
3. **Document assumptions** - explain your data cleaning decisions
4. **Monitor Airflow logs** - they're essential for debugging
5. **Focus on robustness** - handle edge cases gracefully

## Directory Structure

```
t1-coding-challenge-student-data-engineer/
├── dags/                           # Airflow DAG files
│   ├── energy_data.csv            # Sample dataset
│   └── energy_data_pipeline_starter.py  # Your DAG implementation
├── utils/                          # Helper functions
│   ├── __init__.py
│   └── data_helpers.py            # Utility functions
├── processed_data/                 # Partitioned Parquet files (created by pipeline)
├── output/                         # Final outputs and reports (created by pipeline)
├── logs/                          # Airflow logs (created by Docker)
├── docker-compose.yml             # Docker Compose configuration
├── requirements.txt               # Python dependencies
└── README.md                      # This file
```

## Troubleshooting

Common issues and solutions:
- Services not starting: Use `docker compose down -v` then restart
- DAG not visible: Wait for Airflow to scan DAG files or check logs
- Resource issues: Ensure sufficient Docker memory allocation

## Getting Help

- Airflow documentation: https://airflow.apache.org/docs/
- Check Docker logs: `docker compose logs airflow-webserver`
- Airflow UI shows task logs and pipeline status
- Review Docker Compose logs for error messages
- Examine the sample data structure in `dags/energy_data.csv`

## Next Steps

1. Explore the provided dataset and starter code
2. Design and implement your data pipeline
3. Test and validate your solution
