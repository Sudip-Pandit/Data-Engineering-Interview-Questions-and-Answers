# Data Governance / ETL / Modeling Interview Guide

## 73. What is data profiling and why is it important?

### Definition

**Data Profiling** is the process of examining, analyzing, and summarizing data to understand its structure, content, quality, and relationships.

### What Data Profiling Discovers

#### **1. Data Structure**
- Column names and data types
- Number of rows and columns
- File formats and schemas
- Nested structures (JSON, XML)

#### **2. Data Content**
- Value distributions
- Unique values and cardinality
- Patterns and formats
- Sample data

#### **3. Data Quality**
- Null/missing values
- Duplicates
- Outliers
- Invalid values
- Data consistency

#### **4. Data Relationships**
- Primary/foreign key candidates
- Column correlations
- Dependencies between tables
- Referential integrity

### Why Data Profiling is Important

#### **1. Data Quality Assessment**

**Example:**
```
Profile Results:
- Customer Email: 15% NULL values
- Phone Number: 45% invalid format
- Age: Contains values -5, 150, 999 (outliers)

Action: Fix data quality issues before using in analytics
```

#### **2. Data Understanding**

**Before Profiling:**
```
"We have a customer table"
```

**After Profiling:**
```
Customer Table Analysis:
- 2.5M rows
- 45 columns
- customer_id: Unique (100%), Never NULL
- email: 85% populated, 12% duplicates
- registration_date: Range 2015-2024
- age: Mean=35, Median=32, Mode=28
- Top 5 countries: USA (45%), UK (20%), CA (15%), AU (10%), DE (5%)
```

#### **3. ETL Design**

**Profiling informs:**
```
- Data types to use in target
- Transformation logic needed
- Cleansing rules required
- Validation checks to implement
```

**Example:**
```python
# Profile shows SSN has formats:
# - 123-45-6789 (60%)
# - 123456789 (35%)
# - 123 45 6789 (5%)

# Design transformation:
def standardize_ssn(ssn):
    # Remove all non-digits
    clean = re.sub(r'\D', '', ssn)
    # Format as XXX-XX-XXXX
    return f"{clean[:3]}-{clean[3:5]}-{clean[5:]}"
```

#### **4. Migration Planning**

**Source System Profile:**
```
Oracle Database:
- Customer table: 10M rows, 50 columns
- Product table: 500K rows, 30 columns
- Orders table: 100M rows, 20 columns
- Total size: 500GB
```

**Migration Strategy:**
```
1. Incremental load for Orders (large, changes daily)
2. Full load for Products (small, changes rarely)
3. CDC for Customers (medium, changes frequently)
```

#### **5. Data Governance**

**Profiling identifies:**
```
Sensitive Data Found:
- SSN in customers.ssn_number
- Credit Card in payments.card_number
- Email in customers.email_address
- Phone in customers.phone

Action: Apply masking, encryption, access controls
```

### Data Profiling Techniques

#### **1. Column-Level Profiling**

```sql
-- Data type and nullability
SELECT 
    COUNT(*) as total_rows,
    COUNT(email) as non_null_count,
    COUNT(*) - COUNT(email) as null_count,
    ROUND(COUNT(email) * 100.0 / COUNT(*), 2) as fill_rate
FROM customers;

-- Distinct values
SELECT 
    COUNT(DISTINCT country) as distinct_countries,
    COUNT(DISTINCT customer_id) as distinct_customers,
    COUNT(DISTINCT email) as distinct_emails
FROM customers;

-- Value distribution
SELECT 
    country,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM customers
GROUP BY country
ORDER BY count DESC;
```

#### **2. Pattern Analysis**

```sql
-- Email format validation
SELECT 
    CASE 
        WHEN email LIKE '%@%.%' THEN 'Valid Format'
        WHEN email IS NULL THEN 'NULL'
        ELSE 'Invalid Format'
    END as email_status,
    COUNT(*) as count
FROM customers
GROUP BY 
    CASE 
        WHEN email LIKE '%@%.%' THEN 'Valid Format'
        WHEN email IS NULL THEN 'NULL'
        ELSE 'Invalid Format'
    END;

-- Phone number patterns
SELECT 
    SUBSTRING(phone, 1, 3) as area_code,
    COUNT(*) as count
FROM customers
WHERE phone IS NOT NULL
GROUP BY SUBSTRING(phone, 1, 3)
ORDER BY count DESC
LIMIT 10;
```

#### **3. Statistical Analysis**

```sql
-- Numeric column statistics
SELECT 
    MIN(age) as min_age,
    MAX(age) as max_age,
    AVG(age) as avg_age,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY age) as median_age,
    STDDEV(age) as stddev_age
FROM customers;

-- Outlier detection
SELECT *
FROM customers
WHERE age < (SELECT AVG(age) - 3 * STDDEV(age) FROM customers)
   OR age > (SELECT AVG(age) + 3 * STDDEV(age) FROM customers);
```

#### **4. Relationship Analysis**

```sql
-- Foreign key validation
SELECT 
    o.customer_id,
    COUNT(*) as order_count
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL
GROUP BY o.customer_id;
-- Orphaned records

-- Cardinality
SELECT 
    'orders' as source_table,
    'customer_id' as column,
    COUNT(*) as total_records,
    COUNT(DISTINCT customer_id) as distinct_values,
    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT customer_id), 2) as avg_per_value
FROM orders;
```

### Data Profiling Tools

#### **1. Python (Pandas Profiling)**

```python
import pandas as pd
from ydata_profiling import ProfileReport

# Load data
df = pd.read_csv('customers.csv')

# Generate profile
profile = ProfileReport(df, title="Customer Data Profile")

# Save report
profile.to_file("customer_profile.html")

# Report includes:
# - Overview: rows, columns, missing cells, duplicates
# - Variables: type, unique values, missing, statistics
# - Correlations: Pearson, Spearman, Kendall
# - Missing values: count, matrix, heatmap
# - Sample: first/last rows
```

#### **2. SQL-based Profiling**

```sql
-- Comprehensive profile
SELECT 
    'customers' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT customer_id) as unique_customers,
    
    -- Email analysis
    COUNT(email) as email_populated,
    COUNT(DISTINCT email) as unique_emails,
    ROUND(COUNT(email) * 100.0 / COUNT(*), 2) as email_fill_rate,
    
    -- Age statistics
    MIN(age) as min_age,
    MAX(age) as max_age,
    ROUND(AVG(age), 2) as avg_age,
    
    -- Country distribution
    COUNT(DISTINCT country) as unique_countries,
    
    -- Date range
    MIN(registration_date) as earliest_registration,
    MAX(registration_date) as latest_registration
FROM customers;
```

#### **3. Spark (Data Quality)**

```python
from pyspark.sql import functions as F

# Load data
df = spark.read.table("customers")

# Profile
df.select([
    F.count("*").alias("total_rows"),
    F.countDistinct("customer_id").alias("unique_customers"),
    F.sum(F.when(F.col("email").isNull(), 1).otherwise(0)).alias("null_emails"),
    F.avg("age").alias("avg_age"),
    F.min("registration_date").alias("min_date"),
    F.max("registration_date").alias("max_date")
]).show()

# Value distribution
df.groupBy("country").count().orderBy(F.desc("count")).show(10)

# Data quality checks
df.select([
    (F.count("*") - F.count("email")).alias("missing_emails"),
    F.sum(F.when(~F.col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"), 1).otherwise(0)).alias("invalid_emails"),
    F.sum(F.when((F.col("age") < 0) | (F.col("age") > 120), 1).otherwise(0)).alias("invalid_ages")
]).show()
```

### Real-world Example: Customer Data Profile

**Scenario:** Profiling customer data before building analytics pipeline

```python
import pandas as pd

# Load data
df = pd.read_csv('customers.csv')

# Basic info
print(f"Rows: {len(df)}")
print(f"Columns: {len(df.columns)}")
print(f"\nData Types:\n{df.dtypes}")

# Missing values
print(f"\nMissing Values:\n{df.isnull().sum()}")
print(f"\nMissing Percentage:\n{(df.isnull().sum() / len(df) * 100).round(2)}")

# Duplicates
print(f"\nDuplicate Rows: {df.duplicated().sum()}")
print(f"Duplicate Emails: {df['email'].duplicated().sum()}")

# Unique values
print(f"\nCardinality:")
for col in df.columns:
    unique_count = df[col].nunique()
    print(f"{col}: {unique_count} ({unique_count/len(df)*100:.2f}%)")

# Value distributions
print(f"\nTop 5 Countries:\n{df['country'].value_counts().head()}")
print(f"\nAge Statistics:\n{df['age'].describe()}")

# Data quality issues
print(f"\nData Quality Issues:")
print(f"Invalid emails: {(~df['email'].str.match(r'^[\w\.-]+@[\w\.-]+\.\w+$')).sum()}")
print(f"Negative ages: {(df['age'] < 0).sum()}")
print(f"Age > 120: {(df['age'] > 120).sum()}")
print(f"Future registration dates: {(pd.to_datetime(df['registration_date']) > pd.Timestamp.now()).sum()}")
```

**Output:**
```
Rows: 2,500,000
Columns: 15

Data Types:
customer_id         int64
email              object
phone              object
age                 int64
country            object
registration_date  object

Missing Values:
customer_id              0
email              375,000  (15%)
phone              500,000  (20%)
age                 25,000  (1%)
country             10,000  (0.4%)

Duplicate Rows: 50,000
Duplicate Emails: 120,000

Data Quality Issues:
Invalid emails: 85,000
Negative ages: 1,250
Age > 120: 350
Future registration dates: 0

Actions Required:
1. Validate/fix email format (85K records)
2. Handle missing emails (decide: impute, flag, or exclude)
3. Fix negative/extreme ages (1,600 records)
4. Deduplicate emails (120K records)
5. Investigate duplicate rows (50K records)
```

### Best Practices

**1. Profile Early and Often**
```
- Profile source data before extraction
- Profile after each transformation
- Profile final output
- Regular profiling in production
```

**2. Automate Profiling**
```python
# Schedule daily profiling job
def daily_profile():
    df = spark.read.table("production.customers")
    
    profile_results = {
        "date": datetime.now(),
        "row_count": df.count(),
        "null_emails": df.filter(col("email").isNull()).count(),
        "avg_age": df.select(avg("age")).collect()[0][0]
    }
    
    # Store results
    spark.createDataFrame([profile_results]).write.mode("append").table("profiling.daily_stats")
    
    # Alert on anomalies
    if profile_results["null_emails"] > 200000:  # Threshold
        send_alert("High NULL email rate detected")
```

**3. Document Findings**
```
Create data dictionary:
- Column descriptions
- Expected values/ranges
- Data quality rules
- Transformation logic
```

**4. Use Profiling for Validation**
```python
# After ETL transformation
def validate_output(df):
    checks = {
        "row_count_positive": df.count() > 0,
        "no_nulls_in_key": df.filter(col("customer_id").isNull()).count() == 0,
        "valid_email_format": df.filter(~col("email").rlike("@")).count() == 0,
        "age_in_range": df.filter((col("age") < 0) | (col("age") > 120)).count() == 0
    }
    
    if not all(checks.values()):
        raise ValueError(f"Validation failed: {checks}")
```

---

## 74. What is data governance and how do you implement it?

### Definition

**Data Governance** is the overall management of data availability, usability, integrity, and security in an organization.

### Key Components

#### **1. Data Quality**
Ensuring data is accurate, complete, and reliable.

#### **2. Data Security**
Protecting data from unauthorized access.

#### **3. Data Privacy**
Complying with regulations (GDPR, CCPA, HIPAA).

#### **4. Data Lifecycle**
Managing data from creation to deletion.

#### **5. Data Stewardship**
Assigning ownership and responsibilities.

### Implementing Data Governance

#### **Framework**

```
┌─────────────────────────────────────────┐
│        Data Governance Council          │
│    (Executive Sponsors, Data Officers)  │
└─────────────────────────────────────────┘
                    │
        ┌───────────┴───────────┐
        │                       │
┌───────▼───────┐      ┌───────▼────────┐
│ Data Owners   │      │ Data Stewards  │
│ (Business)    │      │ (Technical)    │
└───────────────┘      └────────────────┘
        │                       │
        └───────────┬───────────┘
                    │
        ┌───────────▼───────────┐
        │   Data Users          │
        │ (Analysts, Engineers) │
        └───────────────────────┘
```

#### **1. Define Data Policies**

**Access Policy:**
```yaml
Data Classification:
  Public:
    - Marketing content
    - Public product info
  Internal:
    - Employee directory
    - Department reports
  Confidential:
    - Financial data
    - Customer PII
  Restricted:
    - Trade secrets
    - Merger documents

Access Rules:
  Public: All employees
  Internal: Department members
  Confidential: Approved users only
  Restricted: Executive approval required
```

**Retention Policy:**
```yaml
Data Retention:
  Customer Data:
    Active: Retain indefinitely
    Inactive (3+ years): Archive to cold storage
    Deleted: Purge after 90 days
  
  Transaction Data:
    Current year: Hot storage
    1-7 years: Warm storage (compliance)
    7+ years: Delete (unless legal hold)
  
  Logs:
    Application logs: 90 days
    Security logs: 1 year
    Audit logs: 7 years
```

#### **2. Implement Data Catalog**

**Unity Catalog Example:**
```sql
-- Create catalog structure
CREATE CATALOG production;
CREATE SCHEMA production.sales;
CREATE SCHEMA production.customers;

-- Add metadata
COMMENT ON TABLE production.customers.profiles IS 
'Customer profile information. Owner: Sales Team. 
Refresh: Daily at 2 AM. Contains PII.';

-- Tag sensitive data
ALTER TABLE production.customers.profiles 
SET TAGS ('pii' = 'true', 'sensitivity' = 'high');

-- Document columns
ALTER TABLE production.customers.profiles 
ALTER COLUMN email COMMENT 'Customer email address (PII)';

ALTER TABLE production.customers.profiles 
ALTER COLUMN ssn COMMENT 'Social Security Number (PII - Masked)';
```

#### **3. Implement Access Controls**

**Role-Based Access Control (RBAC):**
```sql
-- Create roles
CREATE ROLE data_analysts;
CREATE ROLE data_engineers;
CREATE ROLE data_scientists;

-- Grant schema-level access
GRANT SELECT ON SCHEMA production.sales TO data_analysts;
GRANT SELECT, MODIFY ON SCHEMA production.sales TO data_engineers;

-- Grant table-level access
GRANT SELECT ON TABLE production.customers.profiles TO data_scientists;

-- Column-level security
GRANT SELECT (customer_id, name, email) 
ON TABLE production.customers.profiles 
TO data_analysts;
-- Analysts cannot see SSN

-- Row-level security
CREATE FUNCTION sales.region_filter(region STRING)
RETURN IF(current_user() = 'manager@company.com', TRUE, region = 'US');

ALTER TABLE production.sales.orders
SET ROW FILTER sales.region_filter ON (region);
```

#### **4. Data Quality Rules**

**Implement Data Quality Checks:**
```python
from pyspark.sql import functions as F

def validate_customer_data(df):
    """Data quality validation rules"""
    
    # Rule 1: Primary key uniqueness
    duplicates = df.groupBy("customer_id").count().filter(F.col("count") > 1)
    assert duplicates.count() == 0, "Duplicate customer_ids found"
    
    # Rule 2: Required fields not null
    null_checks = df.select([
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in ["customer_id", "email", "registration_date"]
    ])
    
    for row in null_checks.collect():
        for field, null_count in row.asDict().items():
            assert null_count == 0, f"{field} has {null_count} null values"
    
    # Rule 3: Email format validation
    invalid_emails = df.filter(
        ~F.col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")
    ).count()
    assert invalid_emails == 0, f"{invalid_emails} invalid email formats"
    
    # Rule 4: Age range validation
    invalid_ages = df.filter((F.col("age") < 0) | (F.col("age") > 120)).count()
    assert invalid_ages == 0, f"{invalid_ages} ages out of valid range"
    
    # Rule 5: Referential integrity
    # Check that all customer_ids in orders exist in customers
    
    return True

# Use in pipeline
df = spark.read.table("bronze.customers")
validate_customer_data(df)
df.write.mode("overwrite").table("silver.customers")
```

#### **5. Data Lineage Tracking**

**Automatic Lineage with Unity Catalog:**
```python
# Create source table
df_raw = spark.read.csv("/data/raw/customers.csv")
df_raw.write.mode("overwrite").saveAsTable("bronze.raw_customers")

# Transform
df_clean = spark.read.table("bronze.raw_customers") \
    .filter(F.col("email").isNotNull()) \
    .withColumn("email_domain", F.split(F.col("email"), "@")[1])

df_clean.write.mode("overwrite").saveAsTable("silver.clean_customers")

# Aggregate
df_summary = spark.read.table("silver.clean_customers") \
    .groupBy("email_domain") \
    .agg(F.count("*").alias("customer_count"))

df_summary.write.mode("overwrite").saveAsTable("gold.customer_summary")

# Lineage automatically tracked:
# bronze.raw_customers → silver.clean_customers → gold.customer_summary
# View in Unity Catalog UI
```

#### **6. Data Masking**

**Implement PII Masking:**
```sql
-- Create masked view
CREATE OR REPLACE VIEW production.customers.profiles_masked AS
SELECT 
    customer_id,
    name,
    CONCAT(LEFT(email, 3), '***@', SUBSTRING_INDEX(email, '@', -1)) as email,
    CONCAT('XXX-XX-', RIGHT(ssn, 4)) as ssn,
    age,
    country
FROM production.customers.profiles;

-- Grant access to masked view
GRANT SELECT ON VIEW production.customers.profiles_masked TO data_analysts;

-- Analysts see:
-- email: joh***@example.com
-- ssn: XXX-XX-1234
```

**Dynamic Masking:**
```python
from pyspark.sql import functions as F

def mask_sensitive_data(df, user_role):
    """Mask data based on user role"""
    
    if user_role == "admin":
        # Admins see everything
        return df
    
    elif user_role == "analyst":
        # Analysts see partial masking
        return df.withColumn(
            "ssn",
            F.concat(F.lit("XXX-XX-"), F.substring(F.col("ssn"), -4, 4))
        ).withColumn(
            "email",
            F.concat(
                F.substring(F.col("email"), 1, 3),
                F.lit("***@"),
                F.split(F.col("email"), "@")[1]
            )
        )
    
    else:
        # Others see full masking
        return df.withColumn("ssn", F.lit("XXX-XX-XXXX")) \
                 .withColumn("email", F.lit("***@***.com"))

# Usage
df = spark.read.table("customers")
df_masked = mask_sensitive_data(df, current_user_role())
```

#### **7. Audit Logging**

**Track Data Access:**
```sql
-- Unity Catalog automatically logs all access
SELECT 
    event_time,
    user_identity.email as user,
    request_params.full_name_arg as table_accessed,
    action_name
FROM system.access.audit
WHERE request_params.full_name_arg LIKE '%customers%'
  AND event_date >= current_date() - 7
ORDER BY event_time DESC;

-- Custom audit log table
CREATE TABLE audit.data_access (
    access_time TIMESTAMP,
    user_name STRING,
    table_name STRING,
    action STRING,
    row_count BIGINT
);

-- Log access in application
INSERT INTO audit.data_access VALUES (
    current_timestamp(),
    current_user(),
    'customers.profiles',
    'SELECT',
    1000
);
```

### Data Governance in Practice

#### **Example: Healthcare Organization**

**Requirements:**
- HIPAA compliance
- Protect PHI (Protected Health Information)
- Audit all access
- Data retention policies

**Implementation:**

```python
# 1. Data Classification
"""
PHI Data:
- patient_id, name, ssn, dob, address, medical_records
- Classification: Restricted
- Encryption: Required
- Access: Healthcare providers only
"""

# 2. Access Control
"""
Roles:
- Doctors: Full access to patient records
- Nurses: Read access to patient records
- Billing: Access to financial data only
- Researchers: De-identified data only
"""

# 3. De-identification for Research
def deidentify_patient_data(df):
    return df.select(
        F.sha2(F.col("patient_id"), 256).alias("patient_hash"),
        F.floor(F.col("age") / 5) * 5).alias("age_group"),  # Age buckets
        F.col("gender"),
        F.col("diagnosis_code"),
        F.col("treatment_date"),
        # Remove: name, SSN, DOB, address, phone
    )

# 4. Audit Logging
def log_phi_access(user, patient_id, action):
    audit_entry = {
        "timestamp": datetime.now(),
        "user": user,
        "patient_id": patient_id,
        "action": action,
        "ip_address": get_client_ip()
    }
    
    spark.createDataFrame([audit_entry]) \
        .write.mode("append") \
        .table("audit.phi_access_log")

# 5. Data Retention
def apply_retention_policy():
    # Keep patient records for 7 years after last visit
    spark.sql("""
        DELETE FROM patients
        WHERE last_visit_date < current_date() - INTERVAL 7 YEARS
          AND NOT EXISTS (
              SELECT 1 FROM legal_holds 
              WHERE legal_holds.patient_id = patients.patient_id
          )
    """)
```

### Key Performance Indicators (KPIs)

**Track Governance Effectiveness:**
```sql
-- Data Quality KPIs
SELECT 
    'Data Quality Score' as metric,
    ROUND(
        (COUNT(*) - 
         SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) -
         SUM(CASE WHEN age < 0 OR age > 120 THEN 1 ELSE 0 END)
        ) * 100.0 / COUNT(*), 2
    ) as value
FROM customers;

-- Access Compliance
SELECT 
    'Users with Quarterly Access Review' as metric,
    ROUND(COUNT(DISTINCT user_id) * 100.0 / (SELECT COUNT(*) FROM users), 2) as percentage
FROM access_reviews
WHERE review_date >= current_date() - INTERVAL 90 DAYS;

-- Data Catalog Coverage
SELECT 
    'Tables with Documentation' as metric,
    ROUND(
        COUNT(CASE WHEN table_comment IS NOT NULL THEN 1 END) * 100.0 / COUNT(*),
        2
    ) as percentage
FROM information_schema.tables;
```

### Best Practices

**1. Start Small, Scale Gradually**
```
Phase 1: Identify critical data
Phase 2: Implement basic access controls
Phase 3: Add data quality checks
Phase 4: Implement full lineage tracking
Phase 5: Mature governance program
```

**2. Automate Where Possible**
```python
# Automated data classification
def classify_column(column_name, sample_values):
    if any(keyword in column_name.lower() for keyword in ['ssn', 'social']):
        return 'PII-High'
    elif any(keyword in column_name.lower() for keyword in ['email', 'phone']):
        return 'PII-Medium'
    elif any(keyword in column_name.lower() for keyword in ['name', 'address']):
        return 'PII-Low'
    else:
        return 'Non-PII'
```

**3. Regular Reviews**
```
- Quarterly access reviews
- Annual policy updates
- Monthly data quality reports
- Weekly security audits
```

**4. Training and Awareness**
```
- Onboarding training for new employees
- Annual refresher courses
- Clear documentation and guidelines
- Regular communication of policies
```

---

## 75. What is OLTP and OLAP and what are the differences?

### OLTP (Online Transaction Processing)

**Definition:** System optimized for managing transaction-oriented applications.

**Characteristics:**
- Handle day-to-day operations
- Many short transactions (INSERT, UPDATE, DELETE)
- Focus on data integrity
- Current, real-time data
- Normalized schema

**Examples:**
- Banking transactions
- E-commerce orders
- Hotel reservations
- Airline bookings

### OLAP (Online Analytical Processing)

**Definition:** System optimized for complex queries and data analysis.

**Characteristics:**
- Handle historical data analysis
- Fewer, complex queries (SELECT with aggregations)
- Focus on query performance
- Historical, aggregated data
- Denormalized schema (star/snowflake)

**Examples:**
- Business intelligence
- Data warehousing
- Financial analysis
- Sales forecasting

### Comparison Table

| Aspect | OLTP | OLAP |
|--------|------|------|
| **Purpose** | Daily operations | Analysis & reporting |
| **Data** | Current, detailed | Historical, summarized |
| **Users** | Many (thousands) | Few (hundreds) |
| **Transactions** | Short, simple | Long, complex |
| **Response Time** | Milliseconds | Seconds to minutes |
| **Database Size** | GB to TB | TB to PB |
| **Schema** | Normalized (3NF) | Denormalized (Star/Snowflake) |
| **Queries** | INSERT, UPDATE, DELETE | SELECT with aggregations |
| **Index** | Few indexes | Many indexes |
| **Backup** | Frequent (continuous) | Less frequent |
| **Example** | Order processing | Sales analysis |

### OLTP Example

**E-commerce Database:**
```sql
-- Normalized schema (3NF)

-- Customers table
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20)
);

-- Orders table
CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT FOREIGN KEY REFERENCES customers(customer_id),
    order_date DATETIME,
    status VARCHAR(20)
);

-- Order_items table
CREATE TABLE order_items (
    item_id INT PRIMARY KEY,
    order_id INT FOREIGN KEY REFERENCES orders(order_id),
    product_id INT FOREIGN KEY REFERENCES products(product_id),
    quantity INT,
    price DECIMAL(10,2)
);

-- Products table
CREATE TABLE products (
    product_id INT PRIMARY KEY,
    name VARCHAR(200),
    category_id INT FOREIGN KEY REFERENCES categories(category_id),
    price DECIMAL(10,2)
);

-- Typical OLTP queries
-- Place new order
INSERT INTO orders (customer_id, order_date, status)
VALUES (12345, GETDATE(), 'pending');

-- Update order status
UPDATE orders 
SET status = 'shipped' 
WHERE order_id = 67890;

-- Get customer orders
SELECT * FROM orders 
WHERE customer_id = 12345 
ORDER BY order_date DESC;
```

### OLAP Example

**Data Warehouse (Star Schema):**
```sql
-- Denormalized schema (Star Schema)

-- Fact table: Sales
CREATE TABLE fact_sales (
    sale_id BIGINT PRIMARY KEY,
    date_key INT FOREIGN KEY REFERENCES dim_date(date_key),
    customer_key INT FOREIGN KEY REFERENCES dim_customer(customer_key),
    product_key INT FOREIGN KEY REFERENCES dim_product(product_key),
    store_key INT FOREIGN KEY REFERENCES dim_store(store_key),
    quantity INT,
    revenue DECIMAL(12,2),
    cost DECIMAL(12,2),
    profit DECIMAL(12,2)
);

-- Dimension table: Date
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE,
    year INT,
    quarter INT,
    month INT,
    month_name VARCHAR(20),
    day_of_week INT,
    day_name VARCHAR(20),
    is_weekend BOOLEAN
);

-- Dimension table: Customer
CREATE TABLE dim_customer (
    customer_key INT PRIMARY KEY,
    customer_id INT,
    name VARCHAR(100),
    age_group VARCHAR(20),
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50),
    customer_segment VARCHAR(30)
);

-- Dimension table: Product
CREATE TABLE dim_product (
    product_key INT PRIMARY KEY,
    product_id INT,
    product_name VARCHAR(200),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    supplier VARCHAR(100)
);

-- Typical OLAP queries
-- Monthly sales by product category
SELECT 
    d.year,
    d.month_name,
    p.category,
    SUM(f.revenue) as total_revenue,
    SUM(f.profit) as total_profit,
    COUNT(DISTINCT f.customer_key) as unique_customers
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_product p ON f.product_key = p.product_key
WHERE d.year = 2024
GROUP BY d.year, d.month_name, p.category
ORDER BY d.year, d.month, total_revenue DESC;

-- Year-over-year growth
SELECT 
    p.category,
    SUM(CASE WHEN d.year = 2023 THEN f.revenue END) as revenue_2023,
    SUM(CASE WHEN d.year = 2024 THEN f.revenue END) as revenue_2024,
    ROUND(
        (SUM(CASE WHEN d.year = 2024 THEN f.revenue END) - 
         SUM(CASE WHEN d.year = 2023 THEN f.revenue END)) * 100.0 /
        SUM(CASE WHEN d.year = 2023 THEN f.revenue END),
        2
    ) as yoy_growth_pct
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_product p ON f.product_key = p.product_key
WHERE d.year IN (2023, 2024)
GROUP BY p.category;
```

### Hybrid: HTAP (Hybrid Transactional/Analytical Processing)

**Modern databases support both:**
```
Azure Synapse Analytics
- OLTP: Dedicated SQL pools
- OLAP: Serverless SQL pools
- Real-time analytics on operational data

Snowflake
- Single platform for both workloads
- Separate compute for isolation

Delta Lake on Databricks
- Supports both batch and streaming
- ACID transactions for reliability
- Optimized for analytics
```

---

## 76. What is a fact table and what is a dimension table?

### Fact Table

**Definition:** Contains quantitative data (metrics/measures) and foreign keys to dimension tables.

**Characteristics:**
- Large number of rows
- Contains measurable events
- Numeric, additive data
- Foreign keys to dimensions
- Grain defines level of detail

**Types of Facts:**
1. **Additive:** Can be summed across all dimensions (revenue, quantity)
2. **Semi-additive:** Can be summed across some dimensions (account balance, inventory)
3. **Non-additive:** Cannot be summed (ratios, percentages)

**Example:**
```sql
CREATE TABLE fact_sales (
    sale_id BIGINT PRIMARY KEY,
    
    -- Foreign keys (dimensions)
    date_key INT,
    customer_key INT,
    product_key INT,
    store_key INT,
    
    -- Facts (measures)
    quantity INT,              -- Additive
    unit_price DECIMAL(10,2),  -- Non-additive
    revenue DECIMAL(12,2),     -- Additive
    cost DECIMAL(12,2),        -- Additive
    profit DECIMAL(12,2),      -- Additive
    discount_pct DECIMAL(5,2)  -- Non-additive
);
```

### Dimension Table

**Definition:** Contains descriptive attributes for analysis and filtering.

**Characteristics:**
- Smaller number of rows
- Contains descriptive data
- Text and categorical fields
- Slowly changing over time
- Used for filtering and grouping

**Types of Dimensions:**
1. **Conformed Dimension:** Shared across multiple fact tables
2. **Degenerate Dimension:** Dimension key in fact table without dimension table
3. **Junk Dimension:** Collection of miscellaneous flags
4. **Role-playing Dimension:** Same dimension used multiple times (order_date, ship_date)

**Example:**
```sql
CREATE TABLE dim_customer (
    customer_key INT PRIMARY KEY,  -- Surrogate key
    customer_id INT,               -- Natural key
    
    -- Descriptive attributes
    name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    address VARCHAR(200),
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50),
    zip_code VARCHAR(10),
    
    -- Classification
    customer_segment VARCHAR(30),  -- Gold, Silver, Bronze
    age_group VARCHAR(20),         -- 18-24, 25-34, etc.
    
    -- SCD Type 2 attributes
    effective_date DATE,
    expiration_date DATE,
    is_current BOOLEAN,
    
    -- Audit columns
    created_date DATETIME,
    updated_date DATETIME
);
```

### Fact vs Dimension

| Aspect | Fact Table | Dimension Table |
|--------|-----------|----------------|
| **Content** | Measures, metrics | Descriptions, attributes |
| **Size** | Large (millions/billions) | Small (thousands) |
| **Data Type** | Mostly numeric | Mostly text |
| **Keys** | FK to dimensions | PK (surrogate key) |
| **Queries** | Aggregated | Filtered, grouped |
| **Updates** | Frequent inserts | Rare updates |
| **Example** | Sales, Orders | Customer, Product |

### Complete Example: Retail Data Warehouse

**Fact Table:**
```sql
CREATE TABLE fact_sales (
    -- Surrogate key
    sale_key BIGINT PRIMARY KEY,
    
    -- Foreign keys to dimensions
    date_key INT REFERENCES dim_date(date_key),
    customer_key INT REFERENCES dim_customer(customer_key),
    product_key INT REFERENCES dim_product(product_key),
    store_key INT REFERENCES dim_store(store_key),
    promotion_key INT REFERENCES dim_promotion(promotion_key),
    
    -- Degenerate dimensions (no dimension table)
    order_number VARCHAR(20),
    line_number INT,
    
    -- Additive facts
    quantity_sold INT,
    revenue_amount DECIMAL(12,2),
    cost_amount DECIMAL(12,2),
    profit_amount DECIMAL(12,2),
    discount_amount DECIMAL(12,2),
    tax_amount DECIMAL(12,2),
    
    -- Non-additive facts
    unit_price DECIMAL(10,2),
    discount_percentage DECIMAL(5,2),
    
    -- Grain: One row per line item per order
    created_timestamp TIMESTAMP
);
```

**Dimension Tables:**
```sql
-- Date Dimension
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE,
    year INT,
    quarter INT,
    month INT,
    month_name VARCHAR(20),
    week INT,
    day_of_month INT,
    day_of_week INT,
    day_name VARCHAR(20),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    fiscal_year INT,
    fiscal_quarter INT
);

-- Customer Dimension (SCD Type 2)
CREATE TABLE dim_customer (
    customer_key INT PRIMARY KEY,
    customer_id INT,
    name VARCHAR(100),
    email VARCHAR(100),
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50),
    customer_segment VARCHAR(30),
    credit_rating VARCHAR(10),
    
    -- SCD Type 2
    effective_date DATE,
    expiration_date DATE,
    is_current BOOLEAN
);

-- Product Dimension
CREATE TABLE dim_product (
    product_key INT PRIMARY KEY,
    product_id INT,
    product_name VARCHAR(200),
    sku VARCHAR(50),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    size VARCHAR(20),
    color VARCHAR(30),
    supplier_name VARCHAR(100),
    unit_cost DECIMAL(10,2),
    list_price DECIMAL(10,2)
);

-- Store Dimension
CREATE TABLE dim_store (
    store_key INT PRIMARY KEY,
    store_id INT,
    store_name VARCHAR(100),
    address VARCHAR(200),
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50),
    region VARCHAR(50),
    store_size VARCHAR(20),  -- Small, Medium, Large
    store_type VARCHAR(30),  -- Mall, Standalone, Outlet
    opening_date DATE
);

-- Promotion Dimension
CREATE TABLE dim_promotion (
    promotion_key INT PRIMARY KEY,
    promotion_id INT,
    promotion_name VARCHAR(100),
    promotion_type VARCHAR(50),  -- BOGO, Percentage Off, Fixed Amount
    discount_percentage DECIMAL(5,2),
    start_date DATE,
    end_date DATE,
    promotion_status VARCHAR(20)
);
```

**Analysis Query:**
```sql
-- Monthly sales by product category and customer segment
SELECT 
    d.year,
    d.month_name,
    p.category,
    c.customer_segment,
    COUNT(DISTINCT f.customer_key) as unique_customers,
    SUM(f.quantity_sold) as total_quantity,
    SUM(f.revenue_amount) as total_revenue,
    SUM(f.profit_amount) as total_profit,
    ROUND(SUM(f.profit_amount) / NULLIF(SUM(f.revenue_amount), 0) * 100, 2) as profit_margin_pct
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_product p ON f.product_key = p.product_key
JOIN dim_customer c ON f.customer_key = c.customer_key
WHERE d.year = 2024
  AND c.is_current = TRUE
GROUP BY d.year, d.month, d.month_name, p.category, c.customer_segment
ORDER BY d.month, total_revenue DESC;
```

---

## 77. What is a primary key and what is a foreign key?

### Primary Key (PK)

**Definition:** Column or set of columns that uniquely identifies each row in a table.

**Characteristics:**
- Must be unique
- Cannot be NULL
- Only one primary key per table
- Indexed automatically
- Used to enforce entity integrity

**Types:**
1. **Natural Key:** Business meaningful (SSN, email)
2. **Surrogate Key:** System-generated (auto-increment ID)

**Examples:**
```sql
-- Single column primary key
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,  -- or IDENTITY(1,1) PRIMARY KEY
    name VARCHAR(100),
    email VARCHAR(100)
);

-- Composite primary key
CREATE TABLE order_items (
    order_id INT,
    line_number INT,
    product_id INT,
    quantity INT,
    PRIMARY KEY (order_id, line_number)
);

-- Named primary key constraint
CREATE TABLE products (
    product_id INT,
    name VARCHAR(200),
    CONSTRAINT pk_products PRIMARY KEY (product_id)
);
```

### Foreign Key (FK)

**Definition:** Column or set of columns that establishes a link between data in two tables.

**Characteristics:**
- References primary key in another table
- Can be NULL (unless specified NOT NULL)
- Multiple foreign keys per table
- Enforces referential integrity
- Can have ON DELETE/UPDATE actions

**Examples:**
```sql
-- Simple foreign key
CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Named foreign key with actions
CREATE TABLE order_items (
    item_id INT PRIMARY KEY,
    order_id INT,
    product_id INT,
    CONSTRAINT fk_order FOREIGN KEY (order_id) 
        REFERENCES orders(order_id)
        ON DELETE CASCADE,  -- Delete items when order deleted
    CONSTRAINT fk_product FOREIGN KEY (product_id)
        REFERENCES products(product_id)
        ON DELETE RESTRICT  -- Prevent product deletion if used
);

-- Composite foreign key
CREATE TABLE enrollments (
    student_id INT,
    course_id INT,
    semester_id INT,
    enrollment_date DATE,
    FOREIGN KEY (course_id, semester_id) 
        REFERENCES course_offerings(course_id, semester_id)
);
```

### Referential Actions

```sql
-- ON DELETE CASCADE: Delete child rows when parent deleted
FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
    ON DELETE CASCADE

-- ON DELETE SET NULL: Set FK to NULL when parent deleted
FOREIGN KEY (manager_id) REFERENCES employees(employee_id)
    ON DELETE SET NULL

-- ON DELETE RESTRICT: Prevent parent deletion if children exist
FOREIGN KEY (product_id) REFERENCES products(product_id)
    ON DELETE RESTRICT

-- ON UPDATE CASCADE: Update FK when parent PK updated
FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
    ON UPDATE CASCADE
```

### Real-world Example

```sql
-- E-commerce schema
CREATE TABLE customers (
    customer_id INT IDENTITY(1,1) PRIMARY KEY,
    email VARCHAR(100) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    registration_date DATE
);

CREATE TABLE orders (
    order_id INT IDENTITY(1,1) PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date DATETIME DEFAULT GETDATE(),
    status VARCHAR(20),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE products (
    product_id INT IDENTITY(1,1) PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    category VARCHAR(50)
);

CREATE TABLE order_items (
    order_id INT,
    line_number INT,
    product_id INT NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10,2) NOT NULL,
    PRIMARY KEY (order_id, line_number),
    FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- Query: Get customer orders with products
SELECT 
    c.name as customer_name,
    o.order_id,
    o.order_date,
    p.name as product_name,
    oi.quantity,
    oi.unit_price,
    oi.quantity * oi.unit_price as line_total
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
WHERE c.customer_id = 12345;
```

---

## 78. What is normalization and why is it used?

### Definition

**Normalization** is the process of organizing data to reduce redundancy and improve data integrity.

### Goals
1. Eliminate redundant data
2. Ensure data dependencies make sense
3. Reduce storage space
4. Prevent update anomalies

### Normal Forms

#### **1st Normal Form (1NF)**

**Rules:**
- Atomic values (no arrays or lists)
- Each column contains single value
- Each row is unique

**Before 1NF:**
```sql
-- Violates 1NF: phone_numbers is multi-valued
CREATE TABLE customers_bad (
    customer_id INT,
    name VARCHAR(100),
    phone_numbers VARCHAR(500)  -- "555-1234, 555-5678, 555-9012"
);
```

**After 1NF:**
```sql
CREATE TABLE customers (
    customer_id INT,
    name VARCHAR(100)
);

CREATE TABLE customer_phones (
    customer_id INT,
    phone_number VARCHAR(20),
    phone_type VARCHAR(20),  -- Home, Mobile, Work
    PRIMARY KEY (customer_id, phone_number)
);
```

#### **2nd Normal Form (2NF)**

**Rules:**
- Must be in 1NF
- No partial dependencies (non-key attributes depend on entire primary key)

**Before 2NF:**
```sql
-- Violates 2NF: product_name depends only on product_id, not full key
CREATE TABLE order_items_bad (
    order_id INT,
    product_id INT,
    product_name VARCHAR(200),  -- Depends only on product_id
    quantity INT,
    PRIMARY KEY (order_id, product_id)
);
```

**After 2NF:**
```sql
CREATE TABLE order_items (
    order_id INT,
    product_id INT,
    quantity INT,
    PRIMARY KEY (order_id, product_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(200)
);
```

#### **3rd Normal Form (3NF)**

**Rules:**
- Must be in 2NF
- No transitive dependencies (non-key attributes depend only on primary key)

**Before 3NF:**
```sql
-- Violates 3NF: country depends on city (transitive)
CREATE TABLE stores_bad (
    store_id INT PRIMARY KEY,
    store_name VARCHAR(100),
    city VARCHAR(50),
    country VARCHAR(50)  -- Transitive: country depends on city
);
```

**After 3NF:**
```sql
CREATE TABLE stores (
    store_id INT PRIMARY KEY,
    store_name VARCHAR(100),
    city_id INT,
    FOREIGN KEY (city_id) REFERENCES cities(city_id)
);

CREATE TABLE cities (
    city_id INT PRIMARY KEY,
    city_name VARCHAR(50),
    country VARCHAR(50)
);
```

### Complete Example: Order Management System

**Unnormalized:**
```sql
CREATE TABLE orders_unnormalized (
    order_id INT,
    order_date DATE,
    customer_name VARCHAR(100),
    customer_email VARCHAR(100),
    customer_city VARCHAR(50),
    customer_state VARCHAR(50),
    products VARCHAR(1000),  -- "Laptop,Mouse,Keyboard"
    quantities VARCHAR(100), -- "1,2,1"
    prices VARCHAR(100)      -- "999.99,29.99,49.99"
);
```

**Normalized (3NF):**
```sql
-- Customers table
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    city VARCHAR(50),
    state VARCHAR(50)
);

-- Orders table
CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Products table
CREATE TABLE products (
    product_id INT PRIMARY KEY,
    name VARCHAR(200),
    price DECIMAL(10,2)
);

-- Order items table
CREATE TABLE order_items (
    order_id INT,
    product_id INT,
    quantity INT,
    PRIMARY KEY (order_id, product_id),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);
```

### Benefits of Normalization

**1. Eliminate Redundancy**
```sql
-- Unnormalized: Customer name repeated in every order
-- Normalized: Customer name stored once in customers table
```

**2. Data Integrity**
```sql
-- Unnormalized: Update customer email in one order, miss others
-- Normalized: Update once in customers table, reflected everywhere
```

**3. Storage Efficiency**
```sql
-- Unnormalized: 1M orders × 200 bytes customer info = 200 MB wasted
-- Normalized: 100K customers × 200 bytes = 20 MB
```

**4. Query Flexibility**
```sql
-- Easy to query customers without orders
SELECT * FROM customers WHERE state = 'CA';

-- Easy to query products that were never ordered
SELECT * FROM products p
WHERE NOT EXISTS (SELECT 1 FROM order_items WHERE product_id = p.product_id);
```

### Denormalization

**When to denormalize:**
- Data warehousing (OLAP)
- Performance optimization
- Reduce joins
- Read-heavy workloads

**Example: Denormalized for Analytics**
```sql
-- Fact table with denormalized dimensions
CREATE TABLE fact_sales_denormalized (
    sale_id BIGINT PRIMARY KEY,
    sale_date DATE,
    
    -- Denormalized customer attributes
    customer_id INT,
    customer_name VARCHAR(100),
    customer_city VARCHAR(50),
    customer_state VARCHAR(50),
    customer_segment VARCHAR(30),
    
    -- Denormalized product attributes
    product_id INT,
    product_name VARCHAR(200),
    product_category VARCHAR(100),
    product_brand VARCHAR(100),
    
    -- Measures
    quantity INT,
    revenue DECIMAL(12,2),
    profit DECIMAL(12,2)
);

-- Fast queries (no joins needed)
SELECT 
    customer_state,
    product_category,
    SUM(revenue) as total_revenue
FROM fact_sales_denormalized
GROUP BY customer_state, product_category;
```

---

## 79. What are facts in ETL?

### Definition

**Facts** are measurable, quantitative data about business events or transactions.

### Characteristics

**1. Numeric:**
```
Examples:
- Sales amount: $125.50
- Quantity sold: 5 units
- Order count: 1
- Duration: 45 minutes
```

**2. Additive or Semi-additive:**
```
Additive: Can sum across all dimensions
- Total revenue = $1,000 + $2,000 + $500 = $3,500

Semi-additive: Can sum across some dimensions
- Average inventory: Can't sum across time

Non-additive: Cannot sum
- Ratios, percentages, temperatures
```

**3. Located in Fact Tables:**
```sql
CREATE TABLE fact_sales (
    -- Keys (not facts)
    date_key INT,
    product_key INT,
    customer_key INT,
    
    -- Facts (measurements)
    quantity_sold INT,          -- Additive
    revenue DECIMAL(12,2),      -- Additive
    cost DECIMAL(12,2),         -- Additive
    profit DECIMAL(12,2),       -- Additive
    discount_pct DECIMAL(5,2),  -- Non-additive
    margin_pct DECIMAL(5,2)     -- Non-additive
);
```

### Types of Facts

#### **1. Additive Facts**
Can be summed across all dimensions.

```sql
-- Examples
revenue, cost, profit, quantity, units_sold, 
amount, count, weight, volume

-- Query
SELECT 
    product_category,
    SUM(revenue) as total_revenue,  -- Valid across all dimensions
    SUM(quantity) as total_quantity
FROM fact_sales
GROUP BY product_category;
```

#### **2. Semi-Additive Facts**
Can be summed across some dimensions, not others.

```sql
-- Examples
account_balance, inventory_level, headcount

-- Inventory example
SELECT 
    product_id,
    SUM(inventory_level)  -- ✓ Valid across products
FROM fact_inventory
WHERE date = '2024-01-15'  -- ✗ Invalid across time
GROUP BY product_id;

-- Correct approach for time
SELECT 
    date,
    AVG(inventory_level)  -- Use average across time
FROM fact_inventory
GROUP BY date;
```

#### **3. Non-Additive Facts**
Cannot be summed at all.

```sql
-- Examples
ratios, percentages, averages, temperatures, 
prices, rates, scores

-- Wrong
SELECT SUM(profit_margin_pct) FROM fact_sales;  -- ✗ Meaningless

-- Correct
SELECT 
    product_category,
    SUM(profit) / NULLIF(SUM(revenue), 0) * 100 as profit_margin_pct
FROM fact_sales
GROUP BY product_category;
```

### Fact Types in ETL

#### **1. Transaction Facts**
One row per business event.

```sql
CREATE TABLE fact_sales (
    sale_id BIGINT PRIMARY KEY,
    date_key INT,
    customer_key INT,
    product_key INT,
    quantity INT,
    amount DECIMAL(12,2)
);

-- Grain: One row per sale transaction
-- Size: Large (millions to billions of rows)
-- Updates: Insert-only (append)
```

#### **2. Periodic Snapshot Facts**
Snapshot at regular intervals.

```sql
CREATE TABLE fact_account_balance (
    account_key INT,
    date_key INT,
    balance DECIMAL(15,2),
    interest_rate DECIMAL(5,2),
    PRIMARY KEY (account_key, date_key)
);

-- Grain: One row per account per day
-- Size: Medium (accounts × days)
-- Updates: Scheduled (daily, monthly)
```

#### **3. Accumulating Snapshot Facts**
Track milestones in a process.

```sql
CREATE TABLE fact_order_fulfillment (
    order_key INT PRIMARY KEY,
    order_date_key INT,
    payment_date_key INT,
    shipment_date_key INT,
    delivery_date_key INT,
    days_to_ship INT,
    days_to_deliver INT,
    order_amount DECIMAL(12,2),
    shipping_cost DECIMAL(10,2)
);

-- Grain: One row per order
-- Size: Small to medium
-- Updates: Updated as milestones complete
```

#### **4. Factless Fact Tables**
Track events without measures.

```sql
CREATE TABLE fact_student_attendance (
    student_key INT,
    course_key INT,
    date_key INT,
    PRIMARY KEY (student_key, course_key, date_key)
);

-- Measures: None (just tracking presence/absence)
-- Query: Count occurrences
SELECT 
    course_key,
    COUNT(*) as attendance_count
FROM fact_student_attendance
GROUP BY course_key;
```

### Calculated Facts

**Stored vs Computed:**

```sql
-- Option 1: Store calculated facts
CREATE TABLE fact_sales (
    quantity INT,
    unit_price DECIMAL(10,2),
    revenue DECIMAL(12,2),      -- Stored: quantity × unit_price
    cost DECIMAL(12,2),
    profit DECIMAL(12,2)         -- Stored: revenue - cost
);

-- Pros: Fast queries (pre-calculated)
-- Cons: More storage, update complexity

-- Option 2: Compute on-the-fly
CREATE TABLE fact_sales (
    quantity INT,
    unit_price DECIMAL(10,2),
    cost DECIMAL(12,2)
);

SELECT 
    quantity * unit_price as revenue,           -- Computed
    (quantity * unit_price) - cost as profit    -- Computed
FROM fact_sales;

-- Pros: Less storage, always accurate
-- Cons: Slower queries (calculation overhead)
```

### ETL Processing of Facts

**Extract:**
```python
# Extract transaction data from source
df_source = spark.read.jdbc(
    url="jdbc:sqlserver://server:1433;database=SourceDB",
    table="dbo.Sales",
    properties={"user": "user", "password": "pass"}
)
```

**Transform:**
```python
from pyspark.sql import functions as F

# Calculate facts
df_transformed = df_source.select(
    F.col("sale_id"),
    F.col("date"),
    F.col("customer_id"),
    F.col("product_id"),
    F.col("quantity"),
    F.col("unit_price"),
    (F.col("quantity") * F.col("unit_price")).alias("revenue"),
    (F.col("quantity") * F.col("unit_cost")).alias("cost"),
    ((F.col("quantity") * F.col("unit_price")) - 
     (F.col("quantity") * F.col("unit_cost"))).alias("profit")
)

# Add surrogate keys (lookup from dimensions)
df_with_keys = df_transformed \
    .join(dim_date, df_transformed.date == dim_date.full_date) \
    .join(dim_customer, df_transformed.customer_id == dim_customer.customer_id) \
    .join(dim_product, df_transformed.product_id == dim_product.product_id) \
    .select(
        F.col("date_key"),
        F.col("customer_key"),
        F.col("product_key"),
        F.col("quantity"),
        F.col("revenue"),
        F.col("cost"),
        F.col("profit")
    )
```

**Load:**
```python
# Load to fact table
df_with_keys.write \
    .mode("append") \
    .format("delta") \
    .saveAsTable("warehouse.fact_sales")
```

---

## 80-85. Rapid Fire Data Modeling Questions

### 80. What is the difference between Star schema and Snowflake schema?

**Star Schema:**
- Denormalized dimensions
- Single level (fact → dimension)
- Simpler queries (fewer joins)
- More storage (redundancy)
- Faster query performance

**Snowflake Schema:**
- Normalized dimensions
- Multiple levels (fact → dimension → sub-dimension)
- Complex queries (more joins)
- Less storage
- Slower query performance

**Example:**

Star Schema:
```sql
fact_sales → dim_product (product_id, name, category, subcategory, brand)
```

Snowflake Schema:
```sql
fact_sales → dim_product (product_id, name, category_id)
           → dim_category (category_id, category_name, subcategory_id)
           → dim_subcategory (subcategory_id, subcategory_name)
```

### 81. What are Slowly Changing Dimensions (SCD) and explain all types

**SCD Type 0:** Never changes (birthdate, SSN)

**SCD Type 1:** Overwrite (no history)
```sql
UPDATE dim_customer 
SET address = 'New Address' 
WHERE customer_id = 123;
-- Old address lost
```

**SCD Type 2:** Add new row (full history)
```sql
-- Mark old record as expired
UPDATE dim_customer 
SET is_current = 0, expiration_date = GETDATE()
WHERE customer_key = 456 AND is_current = 1;

-- Insert new record
INSERT INTO dim_customer (customer_id, address, is_current, effective_date)
VALUES (123, 'New Address', 1, GETDATE());
```

**SCD Type 3:** Add new column (limited history)
```sql
ALTER TABLE dim_customer 
ADD current_address VARCHAR(200), 
    previous_address VARCHAR(200);
```

**SCD Type 4:** Separate history table
**SCD Type 6:** Combination of 1+2+3

### 82. Explain SCD Type 2 with a real-time example

**Scenario:** Customer changes address

```sql
-- Initial state
customer_key | customer_id | name  | address      | is_current | effective_date | expiration_date
1            | 101         | John  | 123 Main St  | 1          | 2020-01-01     | 9999-12-31

-- Customer moves on 2024-01-15
-- Step 1: Expire old record
UPDATE dim_customer 
SET is_current = 0, 
    expiration_date = '2024-01-15'
WHERE customer_key = 1;

-- Step 2: Insert new record
INSERT INTO dim_customer VALUES (
    2,              -- New surrogate key
    101,            -- Same customer_id
    'John',
    '456 Oak Ave',  -- New address
    1,              -- Current
    '2024-01-15',
    '9999-12-31'
);

-- Result: Full history preserved
customer_key | customer_id | name  | address      | is_current | effective_date | expiration_date
1            | 101         | John  | 123 Main St  | 0          | 2020-01-01     | 2024-01-15
2            | 101         | John  | 456 Oak Ave  | 1          | 2024-01-15     | 9999-12-31
```

### 83. How much data are you handling in your current project?

**Template Answer:**

"In my current project, we process approximately:
- **Volume**: 500GB daily, 15TB monthly, 180TB total warehouse
- **Velocity**: 10,000 transactions per second peak
- **Sources**: 25 source systems (SQL Server, Oracle, APIs, flat files)
- **Tables**: 500+ tables (50 fact tables, 200 dimensions)
- **Records**: 5 billion fact records, 50 million dimension records
- **Partitions**: Data partitioned by date (daily partitions)
- **Retention**: 7 years transactional, 10 years aggregates"

### 84. What is the most challenging problem you have faced in your project?

**Template Answer:**

"The most challenging problem was handling late-arriving data in our real-time pipeline:

**Problem:**
- Orders processed in real-time
- Payment confirmations arrived 2-24 hours late
- Reports showed incomplete revenue

**Solution:**
- Implemented SCD Type 2 for fact table
- Added 'last_updated' timestamp
- Created reconciliation process
- Daily batch to update late arrivals

**Result:**
- 99.5% accuracy in real-time reports
- Complete accuracy after daily reconciliation
- Reduced discrepancies from 15% to 0.5%"

### 85. What is the difference between deep copy and shallow copy?

**Shallow Copy:**
- Copies object references, not objects themselves
- Changes to nested objects affect both copies
- Faster, less memory

```python
import copy

original = {'a': 1, 'b': [2, 3, 4]}
shallow = copy.copy(original)

shallow['b'].append(5)
print(original)  # {'a': 1, 'b': [2, 3, 4, 5]} ← Changed!
print(shallow)   # {'a': 1, 'b': [2, 3, 4, 5]}
```

**Deep Copy:**
- Copies objects recursively
- Independent copies
- Slower, more memory

```python
import copy

original = {'a': 1, 'b': [2, 3, 4]}
deep = copy.deepcopy(original)

deep['b'].append(5)
print(original)  # {'a': 1, 'b': [2, 3, 4]} ← Unchanged
print(deep)      # {'a': 1, 'b': [2, 3, 4, 5]}
```

**In Spark:**
```python
# Shallow (reference)
df2 = df1  # Same DataFrame

# Deep (transformation creates new DataFrame)
df2 = df1.select("*")  # Different DataFrame
```

---

*End of Data Governance / ETL / Modeling Guide*

## Summary

This guide covered 13 comprehensive questions on Data Governance, ETL, and Modeling:
1. Data profiling and its importance
2. Data governance implementation
3. OLTP vs OLAP
4. Fact tables and dimension tables
5. Primary keys and foreign keys
6. Normalization
7. Facts in ETL
8-13. Star vs Snowflake schema, SCD types, real-world examples, data volumes, challenges, deep vs shallow copy

Each question includes detailed explanations, SQL examples, real-world scenarios, and best practices.
