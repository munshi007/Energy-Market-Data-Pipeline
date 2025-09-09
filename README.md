# Energy Market Data Pipeline with Apache Airflow

A comprehensive data engineering project that processes energy market bid data using Apache Airflow, featuring market clearing simulation, renewable energy impact analysis, and production-ready monitoring.

## 🎯 Project Overview

This project demonstrates advanced data engineering concepts through a real-world energy trading scenario. It processes 3 days of energy market data (~3,000 records) and provides insights into market dynamics, renewable energy impacts, and trading optimization.

### Key Features

- **Complete Airflow Pipeline**: 10-task DAG with proper dependencies and error handling
- **Market Clearing Simulation**: Advanced supply/demand curve analysis with 98.6% success rate
- **Renewable Energy Analysis**: Impact modeling for +/-2000 MWh scenarios
- **Production-Ready**: Docker containerization, monitoring, and comprehensive logging
- **Financial Analytics**: VWAP calculations, market spread analysis, and efficiency metrics

## 🚀 What You'll Learn

By exploring this project, you'll gain hands-on experience with:

### Data Engineering Skills
- **Apache Airflow**: Workflow orchestration, task dependencies, error handling
- **Data Processing**: Pandas operations, data cleaning, validation techniques
- **Storage Solutions**: Parquet partitioning, data lineage tracking
- **Containerization**: Docker Compose setup for data pipelines

### Financial/Energy Domain Knowledge
- **Market Clearing**: Supply/demand intersection algorithms
- **VWAP Calculations**: Volume-weighted average price computations
- **Renewable Integration**: Price impact analysis and policy insights
- **Risk Management**: Outlier detection and data quality monitoring

### Production Best Practices
- **Error Handling**: Comprehensive try-catch blocks and logging
- **Monitoring**: Pipeline health checks and alerting systems
- **Documentation**: Professional code documentation and setup guides
- **Testing**: Data validation and quality assurance

## 📊 Project Results

The pipeline successfully processes energy market data and generates:

- **Market Analysis**: 73 hours of trading data with equilibrium prices
- **Renewable Insights**: €3.99/MWh average benefit from additional renewables
- **Data Quality**: 98%+ data integrity with automated validation
- **Performance**: 2-3 minute end-to-end execution time

## 🛠️ Technology Stack

- **Orchestration**: Apache Airflow 2.5+
- **Data Processing**: Pandas, NumPy
- **Storage**: Parquet (PyArrow)
- **Containerization**: Docker & Docker Compose
- **Language**: Python 3.8+
- **Monitoring**: Custom health checks and alerting

## 📁 Project Structure

```
energy-market-pipeline/
├── dags/
│   ├── energy_data.csv                    # Sample dataset (3,000 records)
│   └── energy_data_pipeline_starter.py   # Main Airflow DAG
├── utils/
│   └── data_helpers.py                    # 15+ utility functions
├── processed_data/                        # Generated Parquet partitions
├── output/                                # Analysis results and reports
├── docker-compose.yml                     # Container orchestration
├── requirements.txt                       # Python dependencies
└── docs/                                  # Documentation and guides
```

## 🚀 Quick Start

### Prerequisites
- Docker Desktop (4GB+ RAM recommended)
- Git

### Setup (5 minutes)

1. **Clone the repository**
```bash
git clone https://github.com/munshi007/Energy-Market-Data-Pipeline.git
cd Energy-Market-Data-Pipeline
```

2. **Start the pipeline**
```bash
# Initialize Airflow database
docker-compose up airflow-init

# Start all services
docker-compose up -d

# Verify services are running
docker-compose ps
```

3. **Access Airflow UI**
- Open: http://localhost:8080
- Login: `airflow` / `airflow`

4. **Run the pipeline**
- Find `energy_data_pipeline` DAG
- Toggle to "Unpaused"
- Click "Trigger DAG"

### Expected Results

After 2-3 minutes, you'll have:
- ✅ 4 daily Parquet partitions in `processed_data/`
- ✅ Market analysis in `output/final_summary.csv`
- ✅ Comprehensive reports in `output/` directory

## 📈 Business Value & Insights

### Market Analysis Results
- **Equilibrium Success Rate**: 98.6% (72/73 hours)
- **Price Range**: €36.23 - €82.48 per MWh
- **Average Market Efficiency**: 42.7%
- **Total Volume Traded**: 748,375 MWh

### Renewable Energy Impact
- **High Renewable Scenario**: -6.4% average price reduction
- **Low Renewable Scenario**: +4.7% average price increase
- **Policy Insight**: €3.99/MWh economic benefit from renewables
- **Investment ROI**: Clear quantification for renewable projects

## 🎓 Learning Path & Tutorials

### Beginner Level
1. **Start Here**: Follow the Quick Start guide
2. **Explore Data**: Check `dags/energy_data.csv` structure
3. **Understand Flow**: Review the DAG graph in Airflow UI
4. **Check Outputs**: Examine generated files in `output/`

### Intermediate Level
1. **Code Deep Dive**: Study `utils/data_helpers.py` functions
2. **Modify Parameters**: Adjust renewable scenarios (+/-1000 MWh)
3. **Add Metrics**: Implement additional market indicators
4. **Custom Validation**: Create new data quality rules

### Advanced Level
1. **Extend Pipeline**: Add real-time data ingestion
2. **ML Integration**: Implement price prediction models
3. **Scaling**: Deploy on Kubernetes or cloud platforms
4. **Optimization**: Implement parallel processing

## 🔧 Customization Guide

### Modify Renewable Scenarios
```python
# In renewable_impact_analysis function
HIGH_RENEWABLE_SHIFT = 3000  # Change from 2000 MWh
LOW_RENEWABLE_SHIFT = -3000  # Change from -2000 MWh
```

### Add New Metrics
```python
# In calculate_hourly_aggregations
def calculate_custom_metric(df):
    # Your custom market analysis
    return custom_value
```

### Extend Data Sources
```python
# Add new data files
ADDITIONAL_DATA_PATH = '/opt/airflow/dags/weather_data.csv'
# Integrate in validation and cleaning tasks
```

## 📚 Key Concepts Explained

### Market Clearing Algorithm
The pipeline implements a sophisticated market clearing mechanism:
1. **Supply Curve**: Sort sell orders by price (ascending)
2. **Demand Curve**: Sort buy orders by price (descending)
3. **Equilibrium**: Find intersection where supply meets demand
4. **Price Discovery**: Calculate market-clearing price and volume

### VWAP (Volume-Weighted Average Price)
```
VWAP = Σ(Price × Volume) / Σ(Volume)
```
Critical for:
- Fair price assessment
- Trading strategy evaluation
- Market efficiency analysis

### Renewable Impact Modeling
- **Supply Shift**: Additional renewables shift supply curve right
- **Price Effect**: Increased supply typically reduces market prices
- **Policy Value**: Quantifies economic benefits of renewable investments

## 🔍 Troubleshooting

### Common Issues
1. **Services won't start**: Ensure Docker has 4GB+ RAM
2. **DAG not visible**: Wait 30 seconds for Airflow to scan files
3. **Tasks failing**: Check logs in Airflow UI task details
4. **Port conflicts**: Change port 8080 in docker-compose.yml

### Performance Optimization
- **Memory**: Increase Docker memory allocation
- **Parallelism**: Adjust Airflow executor settings
- **Storage**: Use SSD for better I/O performance

## 🤝 Contributing

This project welcomes contributions! Areas for enhancement:

### Data Engineering
- [ ] Add streaming data ingestion (Kafka/Kinesis)
- [ ] Implement data quality monitoring dashboard
- [ ] Add automated testing framework
- [ ] Create CI/CD pipeline

### Analytics
- [ ] Machine learning price prediction
- [ ] Advanced market microstructure analysis
- [ ] Real-time market monitoring
- [ ] Risk management metrics

### Infrastructure
- [ ] Kubernetes deployment manifests
- [ ] Cloud provider templates (AWS/GCP/Azure)
- [ ] Monitoring integration (Prometheus/Grafana)
- [ ] Security hardening

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Energy market data structure inspired by European power exchanges
- Airflow best practices from the Apache Airflow community
- Financial calculations based on industry-standard methodologies

## 📞 Contact

For questions, suggestions, or collaboration opportunities:
- **GitHub Issues**: Use for bug reports and feature requests
- **Discussions**: Share your implementations and improvements
- **LinkedIn**: [Your LinkedIn Profile]

---

**⭐ If this project helped you learn data engineering concepts, please give it a star!**

## 🎯 Next Steps

1. **Fork this repository** at https://github.com/munshi007/Energy-Market-Data-Pipeline
2. **Customize the analysis** for your specific use case
3. **Share your improvements** with the community
4. **Build upon this foundation** for production deployments

Happy data engineering! 🚀