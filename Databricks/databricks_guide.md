# Databricks Interview Guide

## 43. What is Databricks?

### Definition

**Databricks** is a unified analytics platform built on top of Apache Spark that provides a collaborative environment for data engineering, data science, and machine learning.

### Key Components

#### **1. Lakehouse Architecture**
- Combines data lake and data warehouse capabilities
- Stores data in open formats (Delta Lake, Parquet)
- ACID transactions on data lakes
- Direct queries on data lake files

#### **2. Managed Apache Spark**
- Fully managed Spark clusters
- Auto-scaling and auto-termination
- Optimized Spark runtime (3-5x faster than open-source)
- Built-in cluster management

#### **3. Collaborative Workspace**
- Interactive notebooks (Python, Scala, SQL, R)
- Real-time collaboration
- Version control integration (Git)
- Shared dashboards and visualizations

#### **4. Delta Lake Integration**
- Native support for Delta Lake format
- ACID transactions
- Time travel and versioning
- Schema evolution

### Databricks Architecture

```
┌─────────────────────────────────────────────────────┐
│           Databricks Workspace (Web UI)             │
│  ┌──────────┐  ┌──────────┐  ┌────────────────┐   │
│  │Notebooks │  │Workflows │  │  Dashboards    │   │
│  └──────────┘  └──────────┘  └────────────────┘   │
└─────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────┐
│              Control Plane (Managed)                 │
│  - Job Scheduler                                     │
│  - Cluster Manager                                   │
│  - Notebook Execution Engine                         │
│  - Security & Access Control                         │
└─────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────┐
│          Data Plane (Your Cloud Account)            │
│  ┌─────────────┐         ┌────────────────────┐    │
│  │   Clusters  │────────▶│  Cloud Storage     │    │
│  │  (Workers)  │         │  (S3/ADLS/GCS)     │    │
│  └─────────────┘         └────────────────────┘    │
└─────────────────────────────────────────────────────┘
```

### Core Features

#### **1. Clusters**
```python
# Create cluster via UI or API
# Cluster types:
# - All-Purpose: Interactive development
# - Job: Automated workflows
# - SQL Warehouse: SQL analytics
```

**Cluster Modes:**
- **Standard**: Multi-user, shared resources
- **High Concurrency**: Optimized for concurrent queries
- **Single Node**: Development and testing

#### **2. Notebooks**
```python
# Databricks notebook
# Supports: Python, Scala, SQL, R
# Magic commands:

# %python - Python cell
# %scala - Scala cell
# %sql - SQL cell
# %md - Markdown cell
# %sh - Shell commands
# %fs - Databricks filesystem commands
# %run - Run another notebook

# Example:
# %sql
# SELECT * FROM delta.`/mnt/data/table` LIMIT 10

# %python
# df = spark.read.format("delta").load("/mnt/data/table")
# df.show()
```

#### **3. Databricks File System (DBFS)**
```python
# Mount cloud storage
dbutils.fs.mount(
    source = "wasbs://container@account.blob.core.windows.net",
    mount_point = "/mnt/data",
    extra_configs = {"fs.azure.account.key.account.blob.core.windows.net": key}
)

# Access files
dbutils.fs.ls("/mnt/data")

# Read data
df = spark.read.parquet("dbfs:/mnt/data/file.parquet")
# Or
df = spark.read.parquet("/mnt/data/file.parquet")
```

#### **4. Workflows (Jobs)**
- Schedule and orchestrate notebooks
- Parameterize executions
- Monitor and alert
- Retry logic

```python
# Create job via UI or Jobs API
# Define:
# - Cluster configuration
# - Notebook/JAR to run
# - Schedule (cron)
# - Parameters
# - Alerts
```

#### **5. Delta Live Tables (DLT)**
```python
# Declarative ETL framework
import dlt

@dlt.table
def raw_data():
    return spark.read.format("json").load("/mnt/raw")

@dlt.table
def cleaned_data():
    return dlt.read("raw_data").filter(col("value").isNotNull())
```

#### **6. Unity Catalog**
- Centralized data governance
- Fine-grained access control
- Data lineage tracking
- Cross-cloud data sharing

### Databricks vs Apache Spark

| Aspect | Apache Spark | Databricks |
|--------|-------------|------------|
| **Cluster Management** | Manual setup | Managed clusters |
| **Optimization** | Standard Spark | Optimized runtime |
| **Collaboration** | No built-in | Notebooks, real-time collab |
| **Security** | Manual setup | Built-in RBAC |
| **Cost** | Infrastructure only | Platform + Infrastructure |
| **Auto-scaling** | Manual | Automatic |
| **Performance** | Baseline | 3-5x faster |

### Databricks Editions

#### **1. Community Edition (Free)**
- Single user
- Limited cluster size
- No job scheduling
- Basic features

#### **2. Standard**
- Multi-user workspaces
- Job scheduling
- Basic security

#### **3. Premium**
- RBAC
- Audit logs
- SLA guarantees

#### **4. Enterprise**
- Unity Catalog
- Advanced security
- Cross-workspace features
- Dedicated support

### Use Cases

**1. ETL/ELT Pipelines**
```python
# Bronze → Silver → Gold architecture
# Bronze: Raw data ingestion
df_bronze = spark.read.json("/mnt/raw")
df_bronze.write.format("delta").save("/mnt/bronze/table")

# Silver: Cleaned data
df_silver = spark.read.format("delta").load("/mnt/bronze/table") \
    .filter(col("value").isNotNull()) \
    .dropDuplicates()
df_silver.write.format("delta").save("/mnt/silver/table")

# Gold: Business-level aggregates
df_gold = spark.read.format("delta").load("/mnt/silver/table") \
    .groupBy("category").agg(sum("amount"))
df_gold.write.format("delta").save("/mnt/gold/table")
```

**2. Data Science & ML**
```python
# MLflow integration
import mlflow
from sklearn.ensemble import RandomForestClassifier

with mlflow.start_run():
    model = RandomForestClassifier()
    model.fit(X_train, y_train)
    
    mlflow.sklearn.log_model(model, "model")
    mlflow.log_metric("accuracy", accuracy)
```

**3. Real-time Analytics**
```python
# Structured Streaming
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "topic") \
    .load()

df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/checkpoints") \
    .table("streaming_data")
```

**4. SQL Analytics**
```sql
-- Create SQL warehouse
-- Run interactive queries
SELECT 
    category,
    SUM(revenue) as total_revenue,
    COUNT(*) as order_count
FROM delta.`/mnt/gold/orders`
WHERE date >= '2024-01-01'
GROUP BY category
ORDER BY total_revenue DESC
```

### Key Benefits

**1. Productivity**
- No infrastructure management
- Quick cluster provisioning
- Built-in collaboration tools

**2. Performance**
- Optimized Spark runtime
- Automatic query optimization
- Intelligent caching

**3. Cost Optimization**
- Auto-scaling clusters
- Auto-termination
- Spot instance support

**4. Security**
- Built-in RBAC
- Data encryption
- Audit logging
- Compliance certifications

**5. Integration**
- Native cloud integration (AWS, Azure, GCP)
- BI tool connectors
- ML frameworks (MLflow, TensorFlow, PyTorch)
- Version control (Git)

---

## 44. Why do we use Databricks when Apache Spark already exists?

### Key Reasons to Use Databricks

#### **1. Managed Platform (No Infrastructure Management)**

**Apache Spark:**
```bash
# Manual cluster setup
# - Install Spark on each node
# - Configure networking
# - Set up security
# - Monitor and maintain
# - Handle failures manually

# Significant DevOps effort required
```

**Databricks:**
```python
# Click "Create Cluster"
# - Cluster ready in 2-5 minutes
# - Auto-configured and optimized
# - Automatic maintenance
# - Self-healing
```

**Savings:**
- 70% reduction in engineering time
- No dedicated DevOps team needed
- Focus on data, not infrastructure

#### **2. Performance Optimizations**

**Databricks Runtime Enhancements:**
```python
# Photon Engine (C++ vectorized execution)
# - 3-5x faster than OSS Spark
# - Better CPU utilization
# - Automatic query optimization

# Query example:
df = spark.read.format("delta").load("/data/large_table")
result = df.groupBy("category").agg(
    sum("revenue"),
    count("*")
).filter(col("sum(revenue)") > 1000000)

# Apache Spark: 45 seconds
# Databricks: 12 seconds (with Photon)
```

**Automatic Optimizations:**
- Adaptive Query Execution (enhanced)
- Dynamic file pruning
- Low shuffle merge
- Bloom filter joins

#### **3. Collaboration Features**

**Apache Spark:**
- Individual scripts
- Email code sharing
- No real-time collaboration
- Manual version control

**Databricks:**
```python
# Real-time collaborative notebooks
# - Multiple users editing simultaneously
# - Inline comments and discussions
# - Built-in version control
# - Share results instantly

# Example: Team working on same notebook
# User 1: Writing data ingestion code
# User 2: Creating visualizations
# User 3: Reviewing and commenting
# All in real-time!
```

#### **4. Delta Lake Native Support**

**Apache Spark (Manual):**
```python
# Install Delta Lake separately
pip install delta-spark

# Configure Spark session
from delta import *

spark = configure_spark_with_delta_pip(
    SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", 
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
).getOrCreate()
```

**Databricks (Built-in):**
```python
# Delta Lake works out of the box
df.write.format("delta").save("/data/table")
# No configuration needed!

# Advanced features available immediately:
# OPTIMIZE, VACUUM, time travel, MERGE, etc.
```

#### **5. Auto-Scaling and Cost Optimization**

**Apache Spark:**
```python
# Static cluster sizing
# - Provision for peak load
# - Resources idle during low usage
# - Manual scaling required
# - Wasted cost

# Example: Need 100 nodes for 2 hours/day
# But pay for 100 nodes × 24 hours = wasted cost
```

**Databricks:**
```python
# Auto-scaling
# - Cluster scales up during peak
# - Scales down during low usage
# - Auto-terminates when idle
# - Pay only for what you use

# Cluster configuration:
cluster_config = {
    "autoscale": {
        "min_workers": 2,
        "max_workers": 100
    },
    "autotermination_minutes": 30
}

# Cost savings: 40-60% typical
```

#### **6. Integrated Workflows**

**Apache Spark:**
```bash
# External orchestration needed
# - Airflow, Luigi, or other tools
# - Complex setup and maintenance
# - Multiple systems to manage
```

**Databricks:**
```python
# Built-in job scheduling and orchestration
# - Schedule notebooks
# - Create workflows
# - Set dependencies
# - Monitor and alert
# - All in one platform

# Create workflow via UI or API:
{
    "name": "ETL Pipeline",
    "tasks": [
        {
            "task_key": "ingest",
            "notebook_path": "/Notebooks/Ingest"
        },
        {
            "task_key": "transform",
            "depends_on": [{"task_key": "ingest"}],
            "notebook_path": "/Notebooks/Transform"
        },
        {
            "task_key": "aggregate",
            "depends_on": [{"task_key": "transform"}],
            "notebook_path": "/Notebooks/Aggregate"
        }
    ],
    "schedule": {
        "quartz_cron_expression": "0 0 1 * * ?",  # Daily at 1 AM
        "timezone_id": "America/New_York"
    }
}
```

#### **7. Enterprise Security and Governance**

**Apache Spark:**
- Manual security setup
- Custom RBAC implementation
- DIY audit logging
- No built-in governance

**Databricks:**
```python
# Unity Catalog
# - Centralized governance
# - Fine-grained access control
# - Automatic data lineage
# - Audit logging

# Grant permissions:
GRANT SELECT ON TABLE sales TO `data_analysts`;
GRANT ALL PRIVILEGES ON SCHEMA analytics TO `data_engineers`;

# Track lineage automatically
# - Who accessed what data
# - When and from where
# - What transformations applied
```

#### **8. Developer Productivity**

**Apache Spark:**
```python
# Slow development cycle:
# 1. Write code locally
# 2. Package JAR
# 3. Submit to cluster
# 4. Wait for results
# 5. Debug
# 6. Repeat

# Iteration time: 15-30 minutes
```

**Databricks:**
```python
# Fast development cycle:
# 1. Write code in notebook
# 2. Run cell (Shift+Enter)
# 3. See results immediately
# 4. Debug interactively

# Iteration time: seconds

# Interactive exploration:
df = spark.read.table("sales")
display(df)  # Automatic visualization
df.describe().display()  # Quick statistics
```

#### **9. Machine Learning Integration**

**Apache Spark:**
- Separate MLflow setup
- Manual experiment tracking
- Custom model registry
- DIY deployment

**Databricks:**
```python
# MLflow built-in
import mlflow
from sklearn.ensemble import RandomForestClassifier

# Track experiments automatically
with mlflow.start_run():
    model = RandomForestClassifier(n_estimators=100)
    model.fit(X_train, y_train)
    
    # Auto-logged
    accuracy = model.score(X_test, y_test)
    mlflow.log_metric("accuracy", accuracy)
    mlflow.sklearn.log_model(model, "model")

# Model registry included
# Deploy models with one click
# A/B testing built-in
```

#### **10. Debugging and Monitoring**

**Apache Spark:**
```python
# Basic Spark UI
# - Limited metrics
# - No historical data
# - Hard to debug failures
# - Manual log aggregation
```

**Databricks:**
```python
# Enhanced monitoring:
# - Detailed cluster metrics
# - Job history and logs
# - Performance profiling
# - Automatic error highlighting
# - Cost tracking per job

# Example: Debug slow query
# Databricks UI shows:
# - Which stage is slow
# - Data skew visualization
# - Automatic recommendations
# - Detailed execution plan
```

### Cost Comparison Example

**Scenario:** Data engineering team of 5, processing 100TB/month

**Apache Spark (Self-managed):**
```
Infrastructure: $5,000/month
DevOps Engineer (0.5 FTE): $7,500/month
Maintenance & Overhead: $2,000/month
Total: $14,500/month
```

**Databricks:**
```
Infrastructure: $5,000/month
Databricks Platform: $3,000/month
No DevOps needed: $0
Total: $8,000/month

Savings: $6,500/month (45% reduction)
Plus: Faster development, better performance
```

### Real-world Use Case Comparison

**Scenario:** Daily ETL pipeline processing customer data

**Apache Spark Implementation:**
```python
# 1. Manual cluster management
# 2. Write PySpark script
# 3. Schedule with Airflow
# 4. Monitor separately
# 5. Debug on failures
# 6. Manual optimization

# Development time: 2-3 weeks
# Maintenance: 4 hours/week
# Debugging: 2 hours/month
```

**Databricks Implementation:**
```python
# 1. Create notebook
# 2. Write code with interactive feedback
# 3. Schedule as Databricks job
# 4. Built-in monitoring and alerts
# 5. Auto-retry on failures
# 6. Automatic optimizations

# Development time: 3-5 days
# Maintenance: 30 minutes/week
# Debugging: 20 minutes/month

# Bronze layer
@dlt.table
def bronze_customers():
    return spark.read.json("/mnt/raw/customers")

# Silver layer
@dlt.table
def silver_customers():
    return (
        dlt.read("bronze_customers")
        .filter(col("email").isNotNull())
        .dropDuplicates(["customer_id"])
    )

# Gold layer
@dlt.table
def gold_customer_summary():
    return (
        dlt.read("silver_customers")
        .groupBy("region")
        .agg(
            count("*").alias("customer_count"),
            avg("lifetime_value").alias("avg_ltv")
        )
    )
```

### When to Use Apache Spark vs Databricks

**Use Apache Spark When:**
- ❌ Very tight budget constraints
- ❌ Need complete control over infrastructure
- ❌ Have dedicated DevOps team
- ❌ Running on-premises only
- ❌ Simple, one-off processing

**Use Databricks When:**
- ✅ Want to focus on data, not infrastructure
- ✅ Need collaboration features
- ✅ Want faster development cycles
- ✅ Need enterprise security/governance
- ✅ Want performance optimizations
- ✅ Running production workloads
- ✅ Need reliable, scalable platform

### Key Advantages Summary

| Feature | Apache Spark | Databricks |
|---------|-------------|------------|
| **Setup Time** | Days/Weeks | Minutes |
| **Performance** | Baseline | 3-5x faster |
| **Collaboration** | ❌ | ✅ Real-time |
| **Auto-scaling** | Manual | Automatic |
| **Cost Optimization** | Manual | Automatic |
| **Security** | DIY | Enterprise-grade |
| **Monitoring** | Basic | Advanced |
| **Delta Lake** | Manual setup | Built-in |
| **ML Integration** | Separate tools | Integrated |
| **Time to Production** | Weeks/Months | Days/Weeks |

---

## 45. What are the key differences between Databricks and open-source Spark?

### Performance Differences

#### **1. Photon Engine**

**Open-source Spark:**
```python
# JVM-based execution
# Java bytecode interpretation overhead
# GC pauses

# Query time: 45 seconds
df.groupBy("category").agg(sum("revenue")).show()
```

**Databricks (with Photon):**
```python
# C++ vectorized execution engine
# SIMD instructions
# No GC overhead

# Same query time: 8-12 seconds (3-4x faster)
df.groupBy("category").agg(sum("revenue")).show()
```

**Photon Benefits:**
- Vectorized processing
- Better CPU cache utilization
- Reduced memory footprint
- Automatic for Delta Lake and Parquet

#### **2. Optimized I/O**

```python
# Databricks optimizations:
# - Direct cloud storage I/O
# - Predictive prefetching
# - Intelligent caching
# - Optimized Delta Lake read/writes

# Result: 2-3x faster I/O operations
```

### Runtime Enhancements

#### **1. Adaptive Query Execution (Enhanced)**

**Open-source Spark AQE:**
```python
# Basic features:
# - Coalesce partitions
# - Convert to broadcast join
# - Handle data skew

spark.conf.set("spark.sql.adaptive.enabled", "true")
```

**Databricks AQE:**
```python
# Enhanced features:
# - More aggressive optimization
# - Better skew detection
# - Intelligent file pruning
# - Query compilation caching

# Additional optimizations:
# - Low shuffle merge
# - Dynamic runtime filter
# - Bloom filter joins
```

#### **2. Delta Lake Integration**

**Open-source Spark:**
```python
# Install Delta manually
pip install delta-spark==2.4.0

# Configure Spark
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

spark = configure_spark_with_delta_pip(
    SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", 
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
).getOrCreate()

# Limited features
# Manual optimization needed
```

**Databricks:**
```python
# Delta Lake built-in, fully optimized
df.write.format("delta").save("/path")

# Advanced features available:
# - Liquid clustering
# - Deletion vectors
# - Predictive I/O
# - Auto-optimize
# - Enhanced MERGE performance

# Auto-optimize
spark.conf.set("spark.databricks.delta.autoOptimize.optimizeWrite", "true")
spark.conf.set("spark.databricks.delta.autoOptimize.autoCompact", "true")
```

### Platform Features

#### **1. Cluster Management**

**Open-source Spark:**
```bash
# Manual cluster management
# - Install Spark on nodes
# - Configure workers
# - Set up networking
# - Configure security
# - Monitor health
# - Handle failures

# Typical setup time: Days to weeks
```

**Databricks:**
```python
# Click-to-create clusters
# - Auto-configured
# - Self-healing
# - Auto-scaling
# - Auto-termination
# - Pre-installed libraries

# Setup time: 2-5 minutes

# Example cluster config:
{
    "cluster_name": "analytics-cluster",
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "i3.xlarge",
    "autoscale": {
        "min_workers": 2,
        "max_workers": 20
    },
    "autotermination_minutes": 30,
    "enable_elastic_disk": true
}
```

#### **2. Notebooks**

**Open-source Spark:**
```python
# Jupyter notebooks
# - Single user
# - Local execution
# - No real-time collaboration
# - Manual version control
```

**Databricks Notebooks:**
```python
# Enhanced notebooks
# - Multi-user, real-time collaboration
# - Version control built-in
# - Cell-level permissions
# - Magic commands (%sql, %scala, %python, %r)
# - Widgets for parameters
# - Scheduled execution

# Example: Parameterized notebook
dbutils.widgets.text("date", "2024-01-01")
date_param = dbutils.widgets.get("date")

df = spark.read.table("sales").filter(col("date") == date_param)
```

#### **3. Data Management**

**Open-source Spark:**
```python
# Manual catalog management
# External metastore (Hive)
# No built-in governance
# Manual access control
```

**Databricks:**
```python
# Unity Catalog
# - Centralized governance
# - Fine-grained access control
# - Automatic data lineage
# - Cross-workspace sharing
# - Data discovery

# Grant permissions:
GRANT SELECT ON TABLE sales TO `data_analysts`;
GRANT MODIFY ON SCHEMA bronze TO `etl_pipeline`;

# Automatic lineage tracking
# View in UI: Source → Transformations → Destination
```

### Development Experience

#### **1. Interactive Development**

**Open-source Spark:**
```python
# spark-shell or pyspark
# Limited interactivity
# No visualization
# Basic tab completion
```

**Databricks:**
```python
# Rich interactive environment
# - Intelligent autocomplete
# - Inline visualizations
# - Data profiling
# - SQL IntelliSense
# - Error highlighting

# Example: Automatic visualization
display(df)  # Creates interactive chart automatically

# Built-in data profiling
display(df.summary())  # Comprehensive statistics
```

#### **2. Debugging**

**Open-source Spark:**
```python
# Spark UI (basic)
# - Limited metrics
# - No historical data after shutdown
# - Manual log aggregation
```

**Databricks:**
```python
# Advanced debugging tools
# - Enhanced Spark UI
# - Historical cluster logs (30 days+)
# - Automatic error highlighting
# - Performance recommendations
# - Query profiler
# - Cell execution time tracking

# Example: In notebook
%timeit
df.groupBy("category").count().collect()

# Databricks shows:
# - Execution time per stage
# - Bottleneck identification
# - Optimization suggestions
```

### MLOps Integration

**Open-source Spark:**
```python
# Separate MLflow installation
pip install mlflow

# Manual tracking server setup
# Custom model registry
# DIY model serving
```

**Databricks:**
```python
# MLflow fully integrated
import mlflow

# Automatic tracking
mlflow.autolog()

with mlflow.start_run():
    model = train_model()
    # Metrics automatically logged
    
# Model Registry built-in
# - Version control
# - Stage transitions (staging → production)
# - Model serving with one click
# - A/B testing

# Deploy model:
client = mlflow.tracking.MlflowClient()
client.transition_model_version_stage(
    name="my_model",
    version=1,
    stage="Production"
)
```

### Streaming

**Open-source Spark:**
```python
# Structured Streaming (standard)
df = spark.readStream.format("kafka")...

# Basic checkpoint management
# Manual failure recovery
```

**Databricks:**
```python
# Enhanced Structured Streaming
# - Automatic backpressure handling
# - Better fault tolerance
# - Enhanced monitoring

# Delta Live Tables for streaming
import dlt

@dlt.table
def streaming_table():
    return (
        spark.readStream
            .format("cloudFiles")  # Databricks Auto Loader
            .option("cloudFiles.format", "json")
            .load("/mnt/raw")
    )

# Auto Loader features:
# - Automatic schema inference
# - Automatic schema evolution
# - Exactly-once processing
# - Incremental file discovery
```

### Security & Compliance

**Open-source Spark:**
```python
# Manual security setup
# - Configure Kerberos
# - Set up SSL/TLS
# - Implement RBAC
# - Audit logging (custom)
```

**Databricks:**
```python
# Enterprise security built-in
# - RBAC (table, column, row level)
# - Audit logs (automatic)
# - Data encryption (at rest & in transit)
# - IP access lists
# - Private Link / VPC endpoints
# - Compliance certifications (SOC 2, HIPAA, etc.)

# Example: Column-level security
CREATE TABLE sales (
    customer_id INT,
    revenue DOUBLE,
    ssn STRING MASKED
);

GRANT SELECT (customer_id, revenue) ON sales TO `analysts`;
# analysts cannot see ssn column
```

### Monitoring & Observability

**Open-source Spark:**
```python
# Spark metrics system
# - Basic JMX metrics
# - External monitoring needed (Prometheus, Grafana)
# - Manual dashboard setup
```

**Databricks:**
```python
# Comprehensive monitoring
# - Built-in metrics dashboards
# - Job/cluster cost tracking
# - Query history and analysis
# - Performance insights
# - Automatic anomaly detection

# Metrics available:
# - Cluster CPU/memory usage
# - Job duration trends
# - Cost per job
# - Shuffle metrics
# - Cache hit rates
# - Data skew detection
```

### Workflow Orchestration

**Open-source Spark:**
```bash
# External orchestration required
# - Apache Airflow
# - Luigi
# - Oozie
# - Custom scripts

# Additional infrastructure to manage
```

**Databricks:**
```python
# Workflows built-in
# - Job scheduling
# - Task dependencies
# - Parameterization
# - Retries and alerts
# - Email notifications

# Create multi-task workflow:
{
    "name": "ETL Pipeline",
    "tasks": [
        {
            "task_key": "ingest",
            "notebook_task": {
                "notebook_path": "/Notebooks/Ingest",
                "base_parameters": {"date": "{{job.start_date}}"}
            }
        },
        {
            "task_key": "transform",
            "depends_on": [{"task_key": "ingest"}],
            "notebook_task": {
                "notebook_path": "/Notebooks/Transform"
            }
        }
    ],
    "schedule": {
        "quartz_cron_expression": "0 0 * * * ?",
        "timezone_id": "UTC"
    },
    "email_notifications": {
        "on_failure": ["team@company.com"]
    }
}
```

### Cost Optimization

**Open-source Spark:**
```python
# Manual optimization
# - Size clusters manually
# - Monitor usage separately
# - Implement auto-shutdown scripts
# - Manual spot instance management
```

**Databricks:**
```python
# Automatic cost optimization
# - Auto-scaling (scale down when idle)
# - Auto-termination
# - Spot instance integration (automatic failover)
# - Cost tracking per user/job
# - Resource usage dashboards

# Savings typically: 40-60%

# Example auto-scaling config:
{
    "autoscale": {
        "min_workers": 2,
        "max_workers": 50
    },
    "autotermination_minutes": 30,
    "spot_bid_price_percent": 100,  # Use spot instances
    "enable_elastic_disk": true
}
```

### API & SDK Differences

**Open-source Spark:**
```python
# Standard Spark APIs only
from pyspark.sql import SparkSession
```

**Databricks:**
```python
# Additional APIs and utilities
from pyspark.sql import SparkSession
import databricks.koalas as ks  # Pandas-like API on Spark
from databricks.feature_store import FeatureStoreClient

# dbutils (Databricks utilities)
dbutils.fs.ls("/mnt")  # Filesystem operations
dbutils.secrets.get(scope="prod", key="api_key")  # Secret management
dbutils.widgets.text("param", "default")  # Notebook widgets
dbutils.notebook.run("other_notebook", 60)  # Notebook orchestration

# Feature Store
fs = FeatureStoreClient()
fs.create_table(
    name="user_features",
    primary_keys=["user_id"],
    df=features_df
)
```

### Comparison Summary Table

| Feature | Open-source Spark | Databricks |
|---------|------------------|------------|
| **Performance** | Baseline | 3-5x faster (Photon) |
| **Setup Time** | Days/Weeks | Minutes |
| **Auto-scaling** | Manual | Automatic |
| **Notebooks** | Jupyter (basic) | Collaborative, enhanced |
| **Delta Lake** | Manual setup | Built-in, optimized |
| **Security** | DIY | Enterprise-grade |
| **Monitoring** | Basic Spark UI | Advanced dashboards |
| **MLOps** | Separate tools | Integrated MLflow |
| **Governance** | Manual | Unity Catalog |
| **Workflows** | External (Airflow) | Built-in |
| **Cost Tracking** | Manual | Automatic |
| **Support** | Community | Enterprise SLA |
| **Updates** | Manual | Automatic |

---

## 46. What is Unity Catalog in Databricks?

### Definition

**Unity Catalog** is Databricks' unified governance solution for data and AI assets across multiple clouds and workspaces. It provides centralized access control, auditing, lineage, and data discovery.

### Architecture

```
┌────────────────────────────────────────────────────┐
│              Unity Catalog Metastore               │
│  (Centralized metadata and governance)             │
├────────────────────────────────────────────────────┤
│  Catalogs                                          │
│  ├── Production Catalog                            │
│  │   ├── bronze (schema)                           │
│  │   │   └── raw_events (table)                    │
│  │   ├── silver (schema)                           │
│  │   │   └── cleaned_events (table)                │
│  │   └── gold (schema)                             │
│  │       └── aggregated_metrics (table)            │
│  ├── Development Catalog                           │
│  └── Sandbox Catalog                               │
└────────────────────────────────────────────────────┘
         ↓                    ↓                   ↓
   Workspace 1          Workspace 2         Workspace 3
```

### Hierarchy

**Three-level namespace:**
```sql
catalog.schema.table

-- Examples:
production.sales.orders
development.staging.temp_data
sandbox.experiments.test_table
```

**Levels:**
1. **Catalog**: Top-level container (e.g., prod, dev, sandbox)
2. **Schema (Database)**: Logical grouping of tables
3. **Table/View**: Actual data objects

### Key Features

#### **1. Centralized Access Control**

**Table-level permissions:**
```sql
-- Grant SELECT on table
GRANT SELECT ON TABLE production.sales.orders TO `data_analysts`;

-- Grant ALL PRIVILEGES
GRANT ALL PRIVILEGES ON TABLE production.sales.orders TO `data_engineers`;

-- Revoke permissions
REVOKE SELECT ON TABLE production.sales.orders FROM `data_analysts`;
```

**Schema-level permissions:**
```sql
-- Grant on entire schema
GRANT USE SCHEMA ON SCHEMA production.sales TO `analysts`;
GRANT CREATE TABLE ON SCHEMA production.bronze TO `etl_jobs`;
```

**Catalog-level permissions:**
```sql
-- Grant on entire catalog
GRANT USE CATALOG ON CATALOG production TO `all_users`;
GRANT CREATE SCHEMA ON CATALOG development TO `developers`;
```

**Column-level security:**
```sql
-- Create table with masked columns
CREATE TABLE production.customers (
    customer_id INT,
    name STRING,
    email STRING,
    ssn STRING MASKED  -- Masked column
);

-- Grant access to specific columns only
GRANT SELECT (customer_id, name, email) ON TABLE production.customers TO `support_team`;
-- support_team cannot see ssn
```

**Row-level security:**
```sql
-- Create row filter function
CREATE FUNCTION production.filters.regional_filter(region STRING)
RETURN IF(current_user() = 'manager@company.com', true, region = 'US');

-- Apply to table
ALTER TABLE production.sales.orders
SET ROW FILTER production.filters.regional_filter ON (region);

-- Users can only see rows for their region
```

#### **2. Data Lineage**

**Automatic lineage tracking:**
```python
# Create tables with transformations
# Lineage is automatically captured

# Source
df_raw = spark.read.table("production.bronze.raw_data")

# Transform
df_clean = df_raw.filter(col("value").isNotNull()) \
                 .dropDuplicates()

# Write
df_clean.write.mode("overwrite").saveAsTable("production.silver.clean_data")

# Unity Catalog tracks:
# production.bronze.raw_data → transformation → production.silver.clean_data
```

**View lineage in UI:**
- Upstream dependencies
- Downstream consumers
- Transformation details
- User who ran the transformation
- Timestamp

#### **3. Data Discovery**

```sql
-- Search for tables
SHOW TABLES IN production.sales;

-- Get table information
DESCRIBE TABLE EXTENDED production.sales.orders;

-- Search across catalogs
SELECT * FROM system.information_schema.tables
WHERE table_name LIKE '%customer%';

-- View column details
DESCRIBE TABLE production.sales.orders;
```

**Data Explorer UI:**
- Browse catalogs, schemas, tables
- View table schema and sample data
- See data lineage
- View access permissions
- Search by name, tags, or description

#### **4. Audit Logging**

```sql
-- All access is automatically logged
-- View audit logs

SELECT 
    event_time,
    user_identity,
    service_name,
    action_name,
    request_params,
    response
FROM system.access.audit
WHERE action_name = 'getTable'
    AND request_params.full_name_arg = 'production.sales.orders'
ORDER BY event_time DESC;
```

**Audited actions:**
- Table reads/writes
- Permission changes
- Schema modifications
- Data access
- Failed access attempts

### Setting Up Unity Catalog

#### **1. Create Catalog**

```sql
-- Create catalog
CREATE CATALOG IF NOT EXISTS production
COMMENT 'Production data catalog';

-- Create schema
CREATE SCHEMA IF NOT EXISTS production.sales
COMMENT 'Sales department data';

-- Create table
CREATE TABLE production.sales.orders (
    order_id STRING,
    customer_id STRING,
    amount DOUBLE,
    order_date DATE
)
USING DELTA
LOCATION 's3://my-bucket/production/sales/orders';
```

#### **2. Manage Permissions**

```sql
-- Grant catalog access
GRANT USE CATALOG ON CATALOG production TO `all_employees`;

-- Grant schema access
GRANT USE SCHEMA ON SCHEMA production.sales TO `sales_team`;
GRANT SELECT ON SCHEMA production.sales TO `sales_analysts`;

-- Grant table access
GRANT SELECT ON TABLE production.sales.orders TO `BI_tools`;
GRANT MODIFY ON TABLE production.sales.orders TO `etl_pipeline`;
```

#### **3. Create External Locations**

```sql
-- Register external storage location
CREATE EXTERNAL LOCATION s3_bronze
URL 's3://my-bucket/bronze/'
WITH (STORAGE CREDENTIAL aws_s3_credential);

-- Grant access to external location
GRANT READ FILES ON EXTERNAL LOCATION s3_bronze TO `data_engineers`;
GRANT WRITE FILES ON EXTERNAL LOCATION s3_bronze TO `etl_jobs`;
```

### Use Cases

#### **Use Case 1: Multi-Environment Setup**

```sql
-- Development environment
CREATE CATALOG development;
CREATE SCHEMA development.sales;

-- Staging environment
CREATE CATALOG staging;
CREATE SCHEMA staging.sales;

-- Production environment
CREATE CATALOG production;
CREATE SCHEMA production.sales;

-- Promote table from dev to prod
CREATE TABLE production.sales.orders
AS SELECT * FROM development.sales.orders;

-- Different permissions per environment
GRANT ALL PRIVILEGES ON CATALOG development TO `developers`;
GRANT SELECT ON CATALOG production TO `developers`;
GRANT ALL PRIVILEGES ON CATALOG production TO `production_admins`;
```

#### **Use Case 2: Data Sharing Across Teams**

```python
# Marketing team creates table
spark.sql("""
    CREATE TABLE production.marketing.campaigns (
        campaign_id STRING,
        name STRING,
        budget DOUBLE,
        start_date DATE
    ) USING DELTA
""")

# Grant access to sales team
spark.sql("""
    GRANT SELECT ON TABLE production.marketing.campaigns TO `sales_team`
""")

# Sales team can now query marketing data
sales_df = spark.read.table("production.marketing.campaigns")
```

#### **Use Case 3: Regulatory Compliance**

```sql
-- PII data with column masking
CREATE TABLE production.customers (
    customer_id STRING,
    name STRING,
    email STRING,
    ssn STRING MASKED,
    credit_card STRING MASKED
) USING DELTA;

-- Only compliance team can see PII
GRANT SELECT ON TABLE production.customers TO `compliance_team`;

-- Customer support sees masked data
GRANT SELECT (customer_id, name, email) ON TABLE production.customers TO `support_team`;

-- Audit all access to PII
SELECT 
    event_time,
    user_identity.email,
    request_params.full_name_arg as table_name
FROM system.access.audit
WHERE request_params.full_name_arg = 'production.customers'
ORDER BY event_time DESC;
```

### Migration to Unity Catalog

```python
# Migrate existing tables to Unity Catalog

# 1. Create target catalog and schema
spark.sql("CREATE CATALOG IF NOT EXISTS production")
spark.sql("CREATE SCHEMA IF NOT EXISTS production.legacy")

# 2. Clone existing table
spark.sql("""
    CREATE TABLE production.legacy.orders
    SHALLOW CLONE hive_metastore.default.orders
""")

# 3. Verify data
df_old = spark.read.table("hive_metastore.default.orders")
df_new = spark.read.table("production.legacy.orders")
assert df_old.count() == df_new.count()

# 4. Update downstream consumers to use new table
# 5. Drop old table after verification
```

### Best Practices

**1. Naming Conventions**
```sql
-- Use clear, descriptive names
-- Bad
CREATE CATALOG c1;
CREATE SCHEMA c1.s1;

-- Good
CREATE CATALOG production;
CREATE SCHEMA production.sales;
CREATE TABLE production.sales.daily_orders;
```

**2. Least Privilege Access**
```sql
-- Grant minimum necessary permissions
-- Don't grant ALL PRIVILEGES unless required

-- Good: Specific permissions
GRANT SELECT ON TABLE production.sales.orders TO `analysts`;

-- Bad: Overly permissive
GRANT ALL PRIVILEGES ON CATALOG production TO `analysts`;
```

**3. Use Groups, Not Individual Users**
```sql
-- Create groups in identity provider (Azure AD, AWS IAM, etc.)
-- Grant permissions to groups

-- Good
GRANT SELECT ON SCHEMA production.sales TO `sales_analysts_group`;

-- Bad (hard to manage)
GRANT SELECT ON SCHEMA production.sales TO `user1@company.com`;
GRANT SELECT ON SCHEMA production.sales TO `user2@company.com`;
-- ... grant to 100 users individually
```

**4. Document Tables**
```sql
-- Add comments and tags
CREATE TABLE production.sales.orders (
    order_id STRING COMMENT 'Unique order identifier',
    amount DOUBLE COMMENT 'Order total in USD'
)
COMMENT 'Daily sales orders from e-commerce platform'
TBLPROPERTIES (
    'owner' = 'sales-team@company.com',
    'data_classification' = 'internal',
    'retention_days' = '2555'  -- 7 years
);
```

**5. Regular Access Reviews**
```sql
-- Periodically review who has access
SHOW GRANTS ON TABLE production.sales.orders;

-- Review audit logs for suspicious activity
SELECT 
    user_identity.email,
    COUNT(*) as access_count
FROM system.access.audit
WHERE action_name = 'getTable'
    AND event_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY user_identity.email
ORDER BY access_count DESC;
```

### Unity Catalog vs Traditional Metastore

| Aspect | Hive Metastore | Unity Catalog |
|--------|---------------|---------------|
| **Scope** | Single workspace | Cross-workspace, cross-cloud |
| **Namespace** | 2-level (schema.table) | 3-level (catalog.schema.table) |
| **Access Control** | Basic | Fine-grained (table, column, row) |
| **Lineage** | None | Automatic |
| **Audit** | Manual | Built-in |
| **Discovery** | Limited | Advanced |
| **Governance** | Minimal | Comprehensive |
| **Sharing** | Complex | Simple (Delta Sharing) |

---

## 47. What is the purpose of Unity Catalog in real-time projects?

### Real-time Project Challenges Unity Catalog Solves

#### **Challenge 1: Data Sprawl**

**Problem:**
```
Company has:
- 500+ tables across 20 workspaces
- No central catalog
- Duplicate tables with different names
- No one knows what data exists where
```

**Solution with Unity Catalog:**
```sql
-- Centralized catalog
-- All tables organized in one place

-- Search for customer data
SELECT 
    catalog_name,
    schema_name,
    table_name,
    comment
FROM system.information_schema.tables
WHERE table_name LIKE '%customer%';

-- Result: Find all customer-related tables across all workspaces
```

**Benefits:**
- Data discovery: Find data in seconds
- Reduce duplication: Identify redundant tables
- Improve data quality: Single source of truth

#### **Challenge 2: Access Management Chaos**

**Problem:**
```
# Without Unity Catalog:
# - Ad-hoc permissions per workspace
# - No centralized control
# - Security incidents from over-permissioning
# - Audit nightmares

# Example: New analyst joins
# Need to grant access to 50+ tables across 10 workspaces
# Manual process takes hours, error-prone
```

**Solution:**
```sql
-- Create group in Unity Catalog
CREATE GROUP IF NOT EXISTS sales_analysts;

-- Add user to group (done in admin console)
-- Grant permissions to group once
GRANT SELECT ON SCHEMA production.sales TO sales_analysts;

-- New analyst automatically gets access to all relevant data
-- Takes 2 minutes, no errors
```

**Real-world Example:**
```sql
-- Financial Services Company
-- Requirement: Separate access for different regions

-- Create regional row filters
CREATE FUNCTION production.filters.na_region_filter(region STRING)
RETURN IF(is_account_group_member('NA_analysts'), region = 'NA', false);

CREATE FUNCTION production.filters.eu_region_filter(region STRING)
RETURN IF(is_account_group_member('EU_analysts'), region = 'EU', false);

-- Apply filters to tables
ALTER TABLE production.sales.transactions
SET ROW FILTER production.filters.na_region_filter ON (region);

-- NA analysts can only see NA data
-- EU analysts can only see EU data
-- Compliance requirements met automatically
```

#### **Challenge 3: Compliance and Auditing**

**Problem:**
```
# Regulatory requirements (GDPR, HIPAA, SOC 2):
# - Track who accessed PII
# - Prove data is protected
# - Demonstrate access controls
# - Audit trail for regulators

# Without Unity Catalog:
# - Manual logging
# - Incomplete audit trails
# - Failed audits
# - Compliance violations
```

**Solution:**
```sql
-- Automatic audit logging
-- Every data access logged automatically

-- Compliance report: Who accessed customer PII?
SELECT 
    event_time,
    user_identity.email,
    request_params.full_name_arg as table_accessed,
    action_name
FROM system.access.audit
WHERE request_params.full_name_arg LIKE '%pii%'
    AND event_date >= '2024-01-01'
ORDER BY event_time DESC;

-- Export audit logs for regulators
CREATE TABLE compliance.audit_reports.q1_2024
AS SELECT * FROM system.access.audit
WHERE event_date BETWEEN '2024-01-01' AND '2024-03-31';
```

**Healthcare Example:**
```sql
-- HIPAA compliance for patient data

-- 1. Mask PHI columns
CREATE TABLE production.healthcare.patients (
    patient_id STRING,
    name STRING MASKED,
    ssn STRING MASKED,
    diagnosis STRING,
    treatment_date DATE
);

-- 2. Grant access only to authorized personnel
GRANT SELECT ON TABLE production.healthcare.patients TO medical_staff;
GRANT SELECT (patient_id, diagnosis, treatment_date) ON TABLE production.healthcare.patients TO researchers;

-- 3. Audit all access
-- Automatic compliance reporting
```

#### **Challenge 4: Data Lineage and Impact Analysis**

**Problem:**
```python
# Scenario: Need to change a source table schema
# Question: What will break?

# Without Unity Catalog:
# - Manual investigation
# - Miss dependent tables
# - Production breakages
# - Hours/days to trace dependencies
```

**Solution:**
```sql
-- View downstream dependencies automatically
-- In Unity Catalog UI:
-- Click table → Lineage tab
-- See all tables that depend on this one

-- Example impact analysis:
# Changing: production.bronze.raw_events
# Will impact:
#   - production.silver.cleaned_events
#   - production.gold.daily_metrics  
#   - production.gold.user_aggregates
#   - reports.dashboard.kpi_summary

-- All identified automatically in seconds
```

**Real-world Scenario:**
```python
# Data engineering team needs to delete old table
# Check if anyone is using it

# Query lineage
lineage_query = """
SELECT 
    entity_id,
    entity_type,
    upstream_tables,
    downstream_tables
FROM system.access.table_lineage
WHERE entity_id = 'production.legacy.old_table'
"""

lineage = spark.sql(lineage_query)

if lineage.select("downstream_tables").first()[0]:
    print("WARNING: Table is still being used!")
    print("Downstream consumers:")
    lineage.select("downstream_tables").show()
else:
    print("Safe to delete - no downstream dependencies")
```

#### **Challenge 5: Cross-Team Data Collaboration**

**Problem:**
```
# Company structure:
# - Data Engineering team (creates tables)
# - Data Science team (needs access)
# - Analytics team (creates dashboards)
# - Product team (needs specific metrics)

# Without Unity Catalog:
# - Each team has own workspace
# - Data silos
# - Manual sharing process
# - Duplicated data
```

**Solution:**
```sql
-- Centralized sharing

-- Data Engineering creates table
CREATE TABLE production.features.user_features (
    user_id STRING,
    ltv_prediction DOUBLE,
    churn_risk DOUBLE,
    last_updated TIMESTAMP
);

-- Grant access to Data Science
GRANT SELECT ON TABLE production.features.user_features TO data_science_team;

-- Grant access to Analytics
GRANT SELECT ON TABLE production.features.user_features TO analytics_team;

-- Everyone uses the same table
-- No duplication
-- Single source of truth
```

**Feature Store Example:**
```python
from databricks.feature_store import FeatureStoreClient

fs = FeatureStoreClient()

# Data Engineering creates features
fs.create_table(
    name='production.features.user_features',
    primary_keys=['user_id'],
    df=features_df,
    description='ML features for user modeling'
)

# Data Science discovers and uses features
training_set = fs.create_training_set(
    df=labels_df,
    feature_lookups=[
        FeatureLookup(
            table_name='production.features.user_features',
            lookup_key='user_id'
        )
    ],
    label='churn_label'
)

# Features automatically tracked in Unity Catalog
# Lineage: raw data → features → model training
```

#### **Challenge 6: Multi-Cloud / Multi-Region Deployment**

**Problem:**
```
# Global company:
# - US region (AWS)
# - EU region (Azure)
# - APAC region (GCP)

# Requirements:
# - Data sovereignty (EU data stays in EU)
# - Unified governance
# - Cross-region analytics
```

**Solution:**
```sql
-- Single Unity Catalog spans all clouds

-- US catalog (AWS)
CREATE CATALOG us_production
MANAGED LOCATION 's3://us-prod-bucket/';

-- EU catalog (Azure)  
CREATE CATALOG eu_production
MANAGED LOCATION 'abfss://eu-prod@account.dfs.core.windows.net/';

-- APAC catalog (GCP)
CREATE CATALOG apac_production
MANAGED LOCATION 'gs://apac-prod-bucket/';

-- Global view (aggregated, anonymized)
CREATE VIEW global.analytics.daily_metrics AS
SELECT 'US' as region, * FROM us_production.metrics.daily
UNION ALL
SELECT 'EU' as region, * FROM eu_production.metrics.daily
UNION ALL  
SELECT 'APAC' as region, * FROM apac_production.metrics.daily;

-- Analysts query global view
-- Data stays in respective regions
-- Unified governance across all
```

### Real-time Use Case: E-commerce Platform

**Scenario:**
```
Company: Large e-commerce platform
Teams: 
- Data Engineering (10 people)
- Data Science (15 people)
- Analytics (20 people)
- Business Intelligence (30 people)

Data:
- 1000+ tables
- 500TB of data
- 3 environments (dev, staging, prod)
```

**Implementation with Unity Catalog:**

```sql
-- 1. Organize data by domain
CREATE CATALOG production;

CREATE SCHEMA production.sales;      -- Sales domain
CREATE SCHEMA production.customers;  -- Customer domain
CREATE SCHEMA production.products;   -- Product catalog
CREATE SCHEMA production.logistics;  -- Shipping/fulfillment

-- 2. Set up access control
CREATE GROUP data_engineers;
CREATE GROUP data_scientists;
CREATE GROUP analysts;
CREATE GROUP bi_tools;

-- Data Engineers: Full access
GRANT ALL PRIVILEGES ON CATALOG production TO data_engineers;

-- Data Scientists: Read access + sandbox
GRANT SELECT ON CATALOG production TO data_scientists;
GRANT ALL PRIVILEGES ON CATALOG sandbox TO data_scientists;

-- Analysts: Read access to specific schemas
GRANT SELECT ON SCHEMA production.sales TO analysts;
GRANT SELECT ON SCHEMA production.customers TO analysts;

-- BI Tools: Read-only service account
GRANT SELECT ON CATALOG production TO bi_tools;

-- 3. Protect PII
CREATE TABLE production.customers.profiles (
    customer_id STRING,
    email STRING MASKED,
    phone STRING MASKED,
    name STRING,
    registration_date DATE
);

-- Only customer service can see PII
GRANT SELECT ON TABLE production.customers.profiles TO customer_service;

-- 4. Create business metrics layer
CREATE VIEW production.metrics.daily_kpis AS
SELECT 
    DATE(order_timestamp) as date,
    COUNT(DISTINCT customer_id) as daily_customers,
    COUNT(*) as order_count,
    SUM(total_amount) as revenue,
    AVG(total_amount) as avg_order_value
FROM production.sales.orders
GROUP BY DATE(order_timestamp);

GRANT SELECT ON VIEW production.metrics.daily_kpis TO ALL USERS;

-- 5. Monitor usage
-- Dashboard shows:
-- - Most accessed tables
-- - Query patterns
-- - Cost per team
-- - Compliance metrics
```

**Results:**
- **Data Discovery:** Time to find data reduced from hours to minutes
- **Access Management:** New user onboarding from 4 hours to 15 minutes
- **Compliance:** Passed audits with complete audit trail
- **Collaboration:** Cross-team data sharing increased 300%
- **Security:** Zero PII leaks (was 3-4 per year before)
- **Cost:** 30% reduction in duplicate data storage

### Real-time Use Case: Financial Services

**Scenario:**
```
Company: Investment bank
Requirements:
- Strict regulatory compliance (SOX, FINRA)
- Separation of duties
- Trade surveillance
- PII protection
```

**Implementation:**

```sql
-- 1. Catalog per business unit
CREATE CATALOG trading;
CREATE CATALOG risk_management;
CREATE CATALOG compliance;

-- 2. Row-level security for traders
CREATE FUNCTION trading.filters.desk_filter(desk_id STRING)
RETURN IF(
    is_account_group_member('trading_admins'),
    true,  -- Admins see all
    desk_id = current_user_desk()  -- Traders see only their desk
);

ALTER TABLE trading.positions.active_positions
SET ROW FILTER trading.filters.desk_filter ON (desk_id);

-- 3. Audit all trade data access
-- Real-time alerts on suspicious access patterns
CREATE OR REFRESH STREAMING TABLE compliance.alerts.suspicious_access
AS SELECT 
    user_identity.email,
    request_params.full_name_arg as table_name,
    COUNT(*) as access_count,
    window.start as window_start
FROM stream(system.access.audit)
WHERE action_name = 'getTable'
    AND request_params.full_name_arg LIKE 'trading.%'
GROUP BY 
    user_identity.email,
    request_params.full_name_arg,
    window(event_time, '5 minutes')
HAVING COUNT(*) > 100;  -- Alert on unusual volume

-- 4. Compliance reporting
CREATE VIEW compliance.reports.sox_audit_trail AS
SELECT 
    event_time,
    user_identity.email as user,
    request_params.full_name_arg as table_name,
    action_name as action,
    response.status_code as status
FROM system.access.audit
WHERE request_params.full_name_arg LIKE 'trading.%'
ORDER BY event_time DESC;

-- Export monthly for regulators
```

**Results:**
- **Compliance:** 100% audit trail coverage
- **Security:** Desk-level isolation enforced
- **Surveillance:** Real-time alerts on anomalous access
- **Efficiency:** Compliance reporting automated (was 40 hours/month manual work)

### Best Practices for Real-time Projects

**1. Start with Clear Taxonomy**
```sql
-- Define naming convention upfront
-- Example: {environment}.{domain}.{dataset}

production.sales.orders
production.customers.profiles
production.products.catalog

development.sales.orders
development.customers.profiles
```

**2. Implement Least Privilege**
```sql
-- Grant minimum necessary permissions
-- Review quarterly

-- Bad: Over-permissive
GRANT ALL PRIVILEGES ON CATALOG production TO analysts;

-- Good: Specific access
GRANT SELECT ON SCHEMA production.sales TO analysts;
```

**3. Use Tags for Data Classification**
```sql
-- Tag sensitive data
ALTER TABLE production.customers.profiles
SET TAGS ('pii' = 'yes', 'sensitivity' = 'high');

-- Query by tags
SELECT table_catalog, table_schema, table_name
FROM system.information_schema.tables
WHERE array_contains(table_tags, 'pii');
```

**4. Automate Access Reviews**
```python
# Monthly access review script
def review_access():
    # Get all tables with PII tag
    pii_tables = spark.sql("""
        SELECT table_name
        FROM system.information_schema.tables  
        WHERE array_contains(table_tags, 'pii')
    """)
    
    for row in pii_tables.collect():
        table = row.table_name
        
        # Get users with access
        grants = spark.sql(f"SHOW GRANTS ON TABLE {table}")
        
        # Send for manager approval
        send_review_email(table, grants)
```

**5. Monitor and Alert**
```sql
-- Set up monitoring dashboard
-- Track:
-- - Failed access attempts
-- - Unusual query patterns  
-- - PII access frequency
-- - Permission changes

CREATE OR REFRESH STREAMING TABLE monitoring.security_alerts AS
SELECT *
FROM stream(system.access.audit)
WHERE response.status_code != 200  -- Failed attempts
    OR (action_name = 'GRANT' OR action_name = 'REVOKE');  -- Permission changes
```

---

## 48. What are Delta Live Tables?

### Definition

**Delta Live Tables (DLT)** is a declarative framework for building reliable, maintainable, and testable data pipelines in Databricks. It simplifies ETL development by handling infrastructure, error handling, and optimization automatically.

### Key Concepts

**Declarative Pipeline:**
```python
import dlt

# Define what you want, not how to do it
@dlt.table(
    comment="Raw clickstream data"
)
def raw_clickstream():
    return spark.read.format("json").load("/mnt/raw/clickstream")

# DLT handles:
# - Incremental processing
# - Error handling
# - Retries
# - Monitoring
```

### DLT vs Traditional Spark

| Aspect | Traditional Spark | Delta Live Tables |
|--------|------------------|-------------------|
| **Style** | Imperative (how) | Declarative (what) |
| **Error Handling** | Manual | Automatic |
| **Monitoring** | Custom | Built-in |
| **Quality Checks** | Manual | Declarative expectations |
| **Dependencies** | Manual | Auto-detected |
| **Incremental** | Manual logic | Automatic |

### Core Features

#### **1. Tables and Views**

```python
# Table (materialized)
@dlt.table
def silver_orders():
    return spark.read.table("bronze_orders").filter(col("amount") > 0)

# View (not materialized)
@dlt.view  
def orders_by_region():
    return dlt.read("silver_orders").groupBy("region").count()

# Streaming table
@dlt.table
def streaming_events():
    return spark.readStream.table("bronze_events")
```

#### **2. Data Quality (Expectations)**

```python
@dlt.table
@dlt.expect("valid_amount", "amount > 0")  # Warn
@dlt.expect_or_drop("valid_email", "email IS NOT NULL")  # Drop invalid
@dlt.expect_or_fail("valid_id", "id IS NOT NULL")  # Fail pipeline
def clean_orders():
    return spark.read.table("raw_orders")
```

#### **3. Incremental Processing**

```python
# Automatic incremental processing
@dlt.table
def incremental_orders():
    return (
        dlt.read_stream("bronze_orders")  # Auto-handles checkpoints
        .filter(col("status") == "completed")
    )
```

### Real-world Example: Bronze → Silver → Gold

```python
import dlt
from pyspark.sql.functions import *

# BRONZE LAYER (Raw data ingestion)
@dlt.table(
    comment="Raw customer data from JSON files",
    table_properties={"quality": "bronze"}
)
def bronze_customers():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("/mnt/raw/customers/")
    )

# SILVER LAYER (Cleaned data)
@dlt.table(
    comment="Cleaned customer data",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_email", "email IS NOT NULL")
@dlt.expect_or_drop("valid_age", "age > 0 AND age < 120")
def silver_customers():
    return (
        dlt.read_stream("bronze_customers")
        .select(
            col("customer_id"),
            lower(trim(col("email"))).alias("email"),
            col("name"),
            col("age"),
            current_timestamp().alias("processed_at")
        )
        .dropDuplicates(["customer_id"])
    )

# GOLD LAYER (Business aggregates)
@dlt.table(
    comment="Customer segments by age group",
    table_properties={"quality": "gold"}
)
def gold_customer_segments():
    return (
        dlt.read("silver_customers")
        .withColumn("age_group",
            when(col("age") < 25, "18-24")
            .when(col("age") < 35, "25-34")
            .when(col("age") < 50, "35-49")
            .otherwise("50+")
        )
        .groupBy("age_group")
        .agg(
            count("*").alias("customer_count"),
            avg("age").alias("avg_age")
        )
    )
```

---

## 49. How do consumers connect to the Gold layer and consume data?

### Connection Methods

#### **1. SQL Queries (Most Common)**

```sql
-- BI tools, notebooks, applications
SELECT * FROM catalog.schema.gold_table
WHERE date >= current_date() - INTERVAL 7 DAYS;
```

#### **2. JDBC/ODBC Connection**

```python
# Python application
import pyodbc

connection = pyodbc.connect(
    "DRIVER={Simba Spark ODBC Driver};"
    "HOST=<databricks-instance>;"
    "PORT=443;"
    "SSL=1;"
    "THRIFT_TRANSPORT=2;"
    "AUTH_MECH=3;"
    "UID=token;"
    "PWD=<access-token>"
)

cursor = connection.cursor()
cursor.execute("SELECT * FROM production.gold.daily_metrics")
```

#### **3. Databricks SQL Warehouse**

```python
# Optimized for SQL analytics
# Consumers use:
# - SQL queries in SQL Editor
# - Dashboards
# - Alerts
# - External BI tools (Tableau, Power BI)
```

#### **4. REST API**

```python
import requests

response = requests.get(
    "https://<instance>.cloud.databricks.com/api/2.0/sql/statements",
    headers={"Authorization": f"Bearer {token}"},
    json={
        "warehouse_id": "<warehouse-id>",
        "statement": "SELECT * FROM production.gold.kpis",
        "wait_timeout": "30s"
    }
)
```

#### **5. Delta Sharing (External Sharing)**

```python
# Share data with external parties
# Without copying data

# Provider shares table
import delta_sharing

# Consumer reads shared table
url = "https://<sharing-server>/shares/<share>/schemas/<schema>/tables/<table>"
df = delta_sharing.load_as_pandas(url)
```

---

## 50. What are Databricks Utilities (dbutils) and how do you use them?

### Categories of dbutils

#### **1. File System Operations (dbutils.fs)**

```python
# List files
dbutils.fs.ls("/mnt/data")

# Copy files
dbutils.fs.cp("/source/file.csv", "/dest/file.csv")

# Move files  
dbutils.fs.mv("/old/path", "/new/path")

# Delete files
dbutils.fs.rm("/path/to/file", recurse=True)

# Read file
content = dbutils.fs.head("/path/to/file.txt")

# Mount storage
dbutils.fs.mount(
    source="wasbs://container@account.blob.core.windows.net",
    mount_point="/mnt/mydata",
    extra_configs={"fs.azure.account.key.<account>.blob.core.windows.net": key}
)
```

#### **2. Secrets Management (dbutils.secrets)**

```python
# Get secret from scope
api_key = dbutils.secrets.get(scope="prod", key="api_key")

# List secrets in scope
dbutils.secrets.list("prod")

# List all scopes
dbutils.secrets.listScopes()

# Use in connection strings
connection_string = f"Server=host;Password={dbutils.secrets.get('prod', 'db_password')}"
```

#### **3. Notebook Workflows (dbutils.notebook)**

```python
# Run another notebook
result = dbutils.notebook.run(
    "/path/to/notebook",
    timeout_seconds=600,
    arguments={"date": "2024-01-01", "mode": "full"}
)

# Exit with value
dbutils.notebook.exit("Success: processed 1000 records")
```

#### **4. Widgets (dbutils.widgets)**

```python
# Create text widget
dbutils.widgets.text("start_date", "2024-01-01", "Start Date")

# Create dropdown
dbutils.widgets.dropdown("environment", "prod", ["dev", "staging", "prod"])

# Get widget value
start_date = dbutils.widgets.get("start_date")

# Use in queries
df = spark.sql(f"SELECT * FROM orders WHERE date >= '{start_date}'")

# Remove widget
dbutils.widgets.remove("start_date")

# Remove all
dbutils.widgets.removeAll()
```

---

## 51. What are Databricks notebooks?

### Key Features

**1. Multi-language Support:**
```python
# Python cell
df = spark.read.table("sales")

# %sql
SELECT * FROM sales WHERE amount > 1000

# %scala
val df = spark.read.table("sales")

# %r
df <- read.df("sales")
```

**2. Magic Commands:**
```
%python  - Python code
%sql     - SQL queries
%scala   - Scala code
%r       - R code
%md      - Markdown documentation
%sh      - Shell commands
%fs      - Filesystem commands (alias for dbutils.fs)
%run     - Run another notebook
```

**3. Collaboration:**
- Real-time co-editing
- Inline comments
- Version control (Git integration)
- Sharing and permissions

**4. Visualization:**
```python
# Automatic visualization
display(df)  # Creates interactive charts

# Custom plots
import matplotlib.pyplot as plt
plt.plot(x, y)
display(plt.gcf())
```

---

## 52. What notebook languages are supported in Databricks and their use cases?

### Supported Languages

#### **1. Python (PySpark)**
**Best for:**
- Data engineering
- Machine learning
- General data processing

```python
from pyspark.sql.functions import col, sum

df = spark.read.table("sales")
result = df.groupBy("category").agg(sum("amount"))
```

#### **2. SQL**
**Best for:**
- Data analysis
- BI queries
- Quick exploration

```sql
SELECT 
    category,
    SUM(amount) as total,
    COUNT(*) as count
FROM sales
GROUP BY category
ORDER BY total DESC
```

#### **3. Scala**
**Best for:**
- Performance-critical code
- Type-safe transformations
- Spark core development

```scala
val df = spark.read.table("sales")
val result = df.groupBy("category").sum("amount")
```

#### **4. R**
**Best for:**
- Statistical analysis
- Data visualization (ggplot2)
- Academic/research workflows

```r
library(SparkR)
df <- read.df("sales")
result <- summarize(groupBy(df, "category"), total = sum(df$amount))
```

---

## 53. What cluster configuration did you use in your Databricks project?

### Typical Configurations

#### **Development Cluster**
```json
{
    "cluster_name": "dev-cluster",
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "i3.xlarge",
    "driver_node_type_id": "i3.xlarge",
    "num_workers": 2,
    "autotermination_minutes": 30,
    "cluster_mode": "standard"
}
```

#### **Production ETL Cluster**
```json
{
    "cluster_name": "prod-etl",
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "i3.2xlarge",
    "driver_node_type_id": "i3.4xlarge",
    "autoscale": {
        "min_workers": 5,
        "max_workers": 50
    },
    "autotermination_minutes": 60,
    "cluster_mode": "high_concurrency",
    "spark_conf": {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.databricks.delta.autoCompact.enabled": "true"
    }
}
```

#### **ML Training Cluster**
```json
{
    "cluster_name": "ml-training",
    "spark_version": "13.3.x-gpu-ml-scala2.12",
    "node_type_id": "g4dn.xlarge",
    "num_workers": 4,
    "autotermination_minutes": 120,
    "spark_conf": {
        "spark.task.resource.gpu.amount": "1"
    }
}
```

### Best Practices

**1. Right-size Workers:**
- 8-32 cores per worker optimal
- 32-64GB memory per worker
- Don't exceed 40-50GB/worker (GC issues)

**2. Use Auto-scaling:**
```python
# Enable for variable workloads
autoscale = {
    "min_workers": 2,
    "max_workers": 20
}
```

**3. Enable Auto-termination:**
```python
# Prevent idle cluster costs
autotermination_minutes = 30
```

**4. Use Spot Instances:**
```python
# 50-90% cost savings
aws_attributes = {
    "first_on_demand": 1,  # Driver on on-demand
    "availability": "SPOT_WITH_FALLBACK",
    "spot_bid_price_percent": 100
}
```

---

## 54. Based on what factors do you decide the Databricks cluster configuration?

### Decision Factors

#### **1. Workload Type**

**Interactive Development:**
- Small cluster (2-4 workers)
- Standard mode
- Auto-termination (30 min)

**Production ETL:**
- Medium/Large cluster (10-50 workers)
- Auto-scaling
- Job cluster (terminates after job)

**ML Training:**
- GPU instances
- Fixed size (for reproducibility)
- Longer auto-termination

**SQL Analytics:**
- Use SQL Warehouse instead
- Serverless compute

#### **2. Data Volume**

```python
# Formula: workers = data_size_gb / target_gb_per_worker

# Example: 1TB data, target 20GB/worker
workers_needed = 1000 / 20 = 50 workers

# With compression (3x):
workers_needed = (1000 / 3) / 20 = ~17 workers
```

#### **3. Processing Complexity**

**Simple transformations:**
- Fewer workers
- Smaller instances

**Complex aggregations/joins:**
- More workers
- Larger memory per worker

**Shuffle-heavy workloads:**
- More workers
- High network bandwidth instances

#### **4. Time Requirements**

**Urgent (< 1 hour):**
- More workers for parallelism

**Batch overnight:**
- Fewer workers to save cost

#### **5. Cost Constraints**

**Budget optimization:**
```python
# Use spot instances
# Auto-scaling (scale down when idle)
# Right-size (don't over-provision)
# Pool clusters across jobs

# Example cost comparison:
# On-demand: 10 x i3.2xlarge x 8 hours = $160/day
# Spot: 10 x i3.2xlarge x 8 hours x 50% = $80/day
# Savings: $80/day = $2,400/month
```

#### **6. Concurrency Needs**

**Multiple users:**
- High Concurrency mode
- More workers

**Single user/job:**
- Standard mode
- Fewer workers

### Example Scenarios

**Scenario 1: Daily ETL (100GB data)**
```json
{
    "node_type_id": "i3.xlarge",
    "autoscale": {"min_workers": 5, "max_workers": 15},
    "spot_instances": true,
    "autotermination_minutes": 60
}
// Rationale:
// - 100GB / 15 workers ≈ 7GB per worker
// - Auto-scale for variable load
// - Spot for cost savings
```

**Scenario 2: Real-time Streaming (24/7)**
```json
{
    "node_type_id": "r5d.xlarge",  // Memory-optimized
    "num_workers": 10,  // Fixed
    "spot_instances": false,  // On-demand for reliability
    "autotermination_minutes": null  // Never terminate
}
// Rationale:
// - Fixed size for predictable performance
// - On-demand for 24/7 reliability
// - Memory-optimized for caching
```

**Scenario 3: Ad-hoc Analysis**
```json
{
    "node_type_id": "i3.xlarge",
    "num_workers": 2,
    "autotermination_minutes": 30
}
// Rationale:
// - Small cluster for cost efficiency
// - Quick termination when idle
```

---

*End of Databricks Guide*

## Summary

This guide covered 12 comprehensive questions on Databricks:
1. What is Databricks?
2. Why use Databricks vs Apache Spark?
3. Key differences between Databricks and open-source Spark
4. Unity Catalog overview
5. Unity Catalog in real-time projects
6. Delta Live Tables
7. Gold layer consumption
8. Databricks Utilities (dbutils)
9. Databricks notebooks
10. Notebook languages and use cases
11. Cluster configuration examples
12. Factors for cluster configuration decisions

Each question includes detailed explanations, code examples, real-world scenarios, and best practices.
