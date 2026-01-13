# Delta Lake / Storage Interview Guide

## 21. How would you resolve an out-of-memory issue in Spark step by step?

### Step-by-Step Troubleshooting Process

#### **Step 1: Identify the Problem**

**Check Spark UI:**
- Navigate to the Stages tab
- Look for failed tasks or slow-running tasks
- Check error messages (look for "OutOfMemoryError", "Java heap space", "GC overhead limit exceeded")

**Check Logs:**
```bash
# YARN logs
yarn logs -applicationId <app_id>

# Check for OOM errors
grep -i "OutOfMemory" spark-executor-*.log
```

#### **Step 2: Analyze Memory Usage**

**Check Current Configuration:**
```python
# Driver memory
spark.conf.get("spark.driver.memory")

# Executor memory
spark.conf.get("spark.executor.memory")

# Memory fractions
spark.conf.get("spark.memory.fraction")
spark.conf.get("spark.memory.storageFraction")
```

**Identify OOM Source:**
- **Driver OOM**: Usually from `collect()`, `toPandas()`, or broadcasting large data
- **Executor OOM**: Usually from shuffles, aggregations, or caching too much data

#### **Step 3: Immediate Solutions**

**Solution 1: Increase Memory**
```bash
# For driver OOM
spark-submit \
  --driver-memory 16G \
  --conf spark.driver.maxResultSize=8G \
  my_script.py

# For executor OOM
spark-submit \
  --executor-memory 32G \
  --conf spark.executor.memoryOverhead=4G \
  my_script.py
```

**Solution 2: Reduce Memory Pressure**
```python
# Stop caching unnecessary data
df.unpersist()

# Avoid collecting large datasets
# Bad
result = large_df.collect()  # OOM!

# Good
large_df.write.parquet("output")  # Write to storage
```

#### **Step 4: Optimize Code**

**Fix 1: Avoid Driver-side Operations**
```python
# Bad: Brings all data to driver
data = df.collect()  # OOM on driver!
result = [process(row) for row in data]

# Good: Process on executors
df_processed = df.rdd.map(process).toDF()
```

**Fix 2: Optimize Shuffles**
```python
# Increase shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "500")

# Or use AQE to auto-optimize
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

**Fix 3: Process Data in Smaller Chunks**
```python
# Bad: Process entire dataset at once
df = spark.read.parquet("huge_data.parquet")
result = df.groupBy("key").agg(collect_list("value"))  # OOM!

# Good: Process in batches
dates = df.select("date").distinct().collect()
for date in dates:
    df_batch = df.filter(col("date") == date.date)
    result_batch = df_batch.groupBy("key").agg(collect_list("value"))
    result_batch.write.mode("append").parquet("output")
```

#### **Step 5: Optimize Storage and Caching**

**Fix Storage Levels:**
```python
from pyspark import StorageLevel

# Bad: MEMORY_ONLY on large dataset
df.persist(StorageLevel.MEMORY_ONLY)  # OOM!

# Good: Use disk spillover
df.persist(StorageLevel.MEMORY_AND_DISK)

# Better: Use serialized storage to save memory
df.persist(StorageLevel.MEMORY_AND_DISK_SER)
```

**Clear Unnecessary Cache:**
```python
# Check what's cached
spark.catalog.clearCache()

# Or unpersist specific DataFrames
df.unpersist()
```

#### **Step 6: Partition Optimization**

**Increase Partitions:**
```python
# More partitions = less data per partition = less memory per task

# Check current partitions
print(df.rdd.getNumPartitions())

# Increase partitions
df_repartitioned = df.repartition(1000)

# Or adjust default parallelism
spark.conf.set("spark.default.parallelism", "1000")
spark.conf.set("spark.sql.shuffle.partitions", "1000")
```

**Calculate Optimal Partitions:**
```python
# Target: 128MB - 1GB per partition
data_size_gb = 500
partition_size_gb = 0.5
optimal_partitions = data_size_gb / partition_size_gb  # 1000

spark.conf.set("spark.sql.shuffle.partitions", "1000")
```

#### **Step 7: Enable Off-Heap Memory**

```python
# Enable off-heap memory
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "10g")

# This memory is not managed by JVM GC
# Reduces GC pressure and OOM risks
```

#### **Step 8: Handle Data Skew**

```python
# Data skew causes some partitions to be much larger

# Identify skew
df.groupBy("key").count().orderBy(col("count").desc()).show()

# Solution: Salting
from pyspark.sql.functions import rand

df_salted = df.withColumn("salt", (rand() * 10).cast("int"))
df_salted = df_salted.withColumn("salted_key", concat(col("key"), lit("_"), col("salt")))

result = df_salted.groupBy("salted_key").agg(sum("value"))
```

#### **Step 9: Optimize Broadcast Joins**

```python
# Check broadcast size
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")

# If broadcast too large, disable it
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Or increase driver memory
spark.conf.set("spark.driver.memory", "16g")
spark.conf.set("spark.driver.maxResultSize", "8g")
```

#### **Step 10: Monitor and Verify**

**Check Spark UI After Changes:**
- Memory usage per executor
- GC time (should be < 10% of task time)
- Shuffle spill (memory and disk)
- Task execution time distribution

**Enable Monitoring:**
```python
# Log memory usage
spark.sparkContext.setLogLevel("INFO")

# Monitor in code
import psutil
print(f"Memory usage: {psutil.Process().memory_info().rss / 1024**3:.2f} GB")
```

### Real-world OOM Scenarios and Solutions

#### **Scenario 1: Driver OOM from collect()**
```python
# Problem
large_df = spark.read.parquet("1TB_data")
result = large_df.collect()  # Driver OOM!

# Solution
large_df.write.parquet("output")
# Or sample data
sample = large_df.sample(0.01).collect()
```

#### **Scenario 2: Executor OOM from groupBy**
```python
# Problem
df.groupBy("user_id").agg(collect_list("events"))  # Executor OOM!

# Solution 1: Increase partitions
spark.conf.set("spark.sql.shuffle.partitions", "1000")

# Solution 2: Process incrementally
df.groupBy("user_id", "date").agg(collect_list("events"))

# Solution 3: Increase executor memory
# --executor-memory 32G
```

#### **Scenario 3: Cache OOM**
```python
# Problem
df.cache()
df.count()  # OOM!

# Solution 1: Use disk persistence
df.persist(StorageLevel.MEMORY_AND_DISK)

# Solution 2: Don't cache if not reused multiple times
# Just recompute instead

# Solution 3: Cache only needed columns
df_cached = df.select("col1", "col2", "col3").cache()
```

#### **Scenario 4: Join OOM**
```python
# Problem
large_df1.join(large_df2, "key")  # OOM during shuffle

# Solution 1: Increase shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "500")

# Solution 2: Increase memory
# --executor-memory 32G

# Solution 3: Broadcast small table
result = large_df1.join(broadcast(small_df2), "key")

# Solution 4: Filter before join
df1_filtered = large_df1.filter(col("active") == True)
df2_filtered = large_df2.filter(col("active") == True)
result = df1_filtered.join(df2_filtered, "key")
```

### Best Practices to Prevent OOM

**1. Right-size Your Cluster**
```bash
# Don't use executors > 40-50GB (GC issues)
# Better: More executors with moderate memory

# Good
--executor-memory 32G --num-executors 20

# Bad
--executor-memory 100G --num-executors 5
```

**2. Use Appropriate File Formats**
```python
# Parquet: Columnar, compressed, efficient
df.write.parquet("output.parquet")

# Avoid: CSV, JSON for large data (memory inefficient)
```

**3. Process Data Incrementally**
```python
# Process by partitions
for date in date_range:
    df_batch = df.filter(col("date") == date)
    process(df_batch)
```

**4. Monitor Proactively**
```python
# Enable metrics
spark.conf.set("spark.eventLog.enabled", "true")
spark.conf.set("spark.eventLog.dir", "hdfs://logs")
```

**5. Use DataFrame API over RDD**
```python
# DataFrames have better memory management
# and optimization through Catalyst
df.groupBy("key").count()  # Good

rdd.map(lambda x: (x[0], 1)).reduceByKey(add)  # Less efficient
```

---

## 22. What is bucketing in Spark and when should you use partitioning vs bucketing?

### Bucketing in Spark

**Definition:** Bucketing is a technique to pre-shuffle and organize data into a fixed number of buckets based on hash values of one or more columns.

#### **How Bucketing Works**
```python
# Create bucketed table
df.write \
  .bucketBy(100, "user_id") \
  .sortBy("timestamp") \
  .mode("overwrite") \
  .saveAsTable("users_bucketed")

# Each bucket contains rows with same hash(user_id) % 100
# Data is pre-sorted within each bucket
```

**Key Characteristics:**
- Fixed number of buckets (files)
- Hash-based distribution
- Data sorted within buckets (optional)
- Metadata stored in metastore

### Partitioning vs Bucketing

| Aspect | Partitioning | Bucketing |
|--------|-------------|-----------|
| **Purpose** | Physical data organization | Query optimization |
| **Directory Structure** | Creates subdirectories | Single directory |
| **Number of Files** | Variable (depends on data) | Fixed (number of buckets) |
| **Based On** | Column values | Hash of column values |
| **Best For** | Range queries, filtering | Joins, aggregations |
| **Shuffle** | Eliminates for partition filters | Eliminates for bucketed joins |
| **Metastore** | Not required | Required |

### Partitioning in Detail

**What is Partitioning:**
```python
# Creates directory structure: year=2024/month=01/
df.write \
  .partitionBy("year", "month") \
  .parquet("output")

# Directory structure:
# output/
#   year=2024/
#     month=01/
#       part-00000.parquet
#       part-00001.parquet
#     month=02/
#       part-00000.parquet
```

**Characteristics:**
- Physical directory structure
- Each partition = subdirectory
- Number of files varies with data
- Partition pruning for queries

**Advantages:**
- ✅ Fast filtering by partition columns
- ✅ Query only relevant partitions
- ✅ Easy to manage time-series data
- ✅ Works with any file format

**Disadvantages:**
- ❌ Too many partitions = small files problem
- ❌ Only works for exact matches and ranges
- ❌ Doesn't help with joins

### Bucketing in Detail

**What is Bucketing:**
```python
# Creates fixed number of files
df.write \
  .bucketBy(100, "user_id") \
  .mode("overwrite") \
  .saveAsTable("users_bucketed")

# Creates 100 files (buckets):
# warehouse/users_bucketed/
#   part-00000-<uuid>
#   part-00001-<uuid>
#   ...
#   part-00099-<uuid>
```

**Characteristics:**
- Fixed number of files
- Hash-based distribution
- Must use saveAsTable() (requires metastore)
- Bucket metadata stored

**Advantages:**
- ✅ Eliminates shuffle for joins on bucketed columns
- ✅ Faster aggregations on bucketed columns
- ✅ Consistent file sizes
- ✅ Pre-sorted data (if sortBy used)

**Disadvantages:**
- ❌ Requires Hive metastore
- ❌ Can't use with write.parquet() directly
- ❌ Harder to manage
- ❌ All tables must have same bucketing scheme

### When to Use Partitioning

**Use Case 1: Time-Series Data**
```python
# Partition by date for time-based filtering
logs.write \
  .partitionBy("year", "month", "day") \
  .parquet("logs")

# Query with partition pruning
df = spark.read.parquet("logs") \
  .filter(col("year") == 2024) \
  .filter(col("month") == 1)
# Only reads year=2024/month=01/ directory
```

**Use Case 2: Category-Based Filtering**
```python
# Partition by region
sales.write \
  .partitionBy("region", "country") \
  .parquet("sales")

# Efficient regional queries
df = spark.read.parquet("sales") \
  .filter(col("region") == "APAC")
```

**Use Case 3: Data Lake Organization**
```python
# Organize data lake by business domains
events.write \
  .partitionBy("event_type", "date") \
  .parquet("s3://data-lake/events")
```

**Best Practices for Partitioning:**
- Partition columns should have low cardinality (< 1000 unique values)
- Avoid over-partitioning (creates too many small files)
- Partition by columns frequently used in WHERE clauses
- Target partition size: 128MB - 1GB

### When to Use Bucketing

**Use Case 1: Frequent Joins on Same Key**
```python
# Bucket both tables by join key
users.write \
  .bucketBy(100, "user_id") \
  .saveAsTable("users_bucketed")

orders.write \
  .bucketBy(100, "user_id") \
  .saveAsTable("orders_bucketed")

# Join without shuffle
users_df = spark.table("users_bucketed")
orders_df = spark.table("orders_bucketed")
result = users_df.join(orders_df, "user_id")
# No shuffle! Much faster
```

**Use Case 2: Frequent Aggregations**
```python
# Bucket by aggregation key
events.write \
  .bucketBy(200, "user_id") \
  .sortBy("timestamp") \
  .saveAsTable("events_bucketed")

# Aggregation without shuffle
df = spark.table("events_bucketed")
result = df.groupBy("user_id").agg(count("*"))
# Pre-bucketed data avoids shuffle
```

**Use Case 3: Large Table Joins**
```python
# For large-large joins where broadcast isn't possible
# Bucketing both tables avoids expensive shuffle

# Bucket large tables
large_table1.write.bucketBy(500, "key").saveAsTable("t1_bucketed")
large_table2.write.bucketBy(500, "key").saveAsTable("t2_bucketed")

# Join is much faster
t1 = spark.table("t1_bucketed")
t2 = spark.table("t2_bucketed")
result = t1.join(t2, "key")
```

**Best Practices for Bucketing:**
- Number of buckets should be a multiple of parallelism
- Bucket columns should be high cardinality (join/aggregation keys)
- Use sortBy for range queries within buckets
- All tables joined together should use same bucketing scheme

### Combining Partitioning and Bucketing

```python
# Best of both worlds
df.write \
  .partitionBy("date") \          # For time-based filtering
  .bucketBy(100, "user_id") \     # For joins/aggregations
  .sortBy("timestamp") \           # For sorted reads
  .saveAsTable("events_optimized")

# Benefits:
# 1. Partition pruning for date filters
# 2. No shuffle for joins on user_id
# 3. Sorted data within buckets
```

### Real-world Examples

#### **Example 1: E-commerce Analytics**
```python
# Partitioning for orders (time-based queries)
orders.write \
  .partitionBy("order_date") \
  .parquet("s3://data/orders")

# Query specific date range
daily_orders = spark.read.parquet("s3://data/orders") \
  .filter(col("order_date").between("2024-01-01", "2024-01-31"))
```

#### **Example 2: User Behavior Analysis**
```python
# Bucketing for user events (frequent joins)
events.write \
  .bucketBy(200, "user_id") \
  .sortBy("event_time") \
  .saveAsTable("user_events_bucketed")

users.write \
  .bucketBy(200, "user_id") \
  .saveAsTable("users_bucketed")

# Join without shuffle
events_df = spark.table("user_events_bucketed")
users_df = spark.table("users_bucketed")
result = events_df.join(users_df, "user_id")
```

#### **Example 3: Combined Approach**
```python
# Partition by date (for time filtering)
# Bucket by user (for user-based aggregations)
clicks.write \
  .partitionBy("date") \
  .bucketBy(100, "user_id") \
  .saveAsTable("clicks_optimized")

# Efficient query with both optimizations
df = spark.table("clicks_optimized") \
  .filter(col("date") == "2024-01-15")  # Partition pruning
result = df.groupBy("user_id").count()  # No shuffle (bucketed)
```

### Decision Matrix

**Use Partitioning When:**
- ✅ Filtering by specific column values frequently
- ✅ Working with time-series data
- ✅ Need physical data organization
- ✅ Want to use with any storage format
- ✅ Don't need Hive metastore

**Use Bucketing When:**
- ✅ Frequent joins on same columns
- ✅ Frequent aggregations on specific columns
- ✅ Large-large table joins
- ✅ Have Hive metastore available
- ✅ Tables don't change frequently

**Use Both When:**
- ✅ Need time-based filtering AND joins/aggregations
- ✅ Large data warehouse with complex queries
- ✅ Want maximum optimization

**Use Neither When:**
- ❌ Data is small (< 1GB)
- ❌ Ad-hoc queries with varying patterns
- ❌ Data changes very frequently

### Common Pitfalls

#### **Pitfall 1: Over-partitioning**
```python
# Bad: Too many partitions
df.write.partitionBy("user_id").parquet("output")
# If 1M users → 1M directories!

# Good: Partition by low cardinality column
df.write.partitionBy("date").parquet("output")
```

#### **Pitfall 2: Wrong Bucket Count**
```python
# Bad: Too few buckets
df.write.bucketBy(5, "user_id").saveAsTable("users")
# Limits parallelism

# Good: More buckets for large data
df.write.bucketBy(200, "user_id").saveAsTable("users")
```

#### **Pitfall 3: Bucketing Without Metastore**
```python
# Bad: Can't use bucketBy with write.parquet()
df.write.bucketBy(100, "key").parquet("output")  # Error!

# Good: Use saveAsTable with metastore
df.write.bucketBy(100, "key").saveAsTable("table_name")
```

---

## 23. Explain data skew and how you handled it.

### What is Data Skew?

**Definition:** Data skew occurs when data is unevenly distributed across partitions, causing some tasks to process significantly more data than others.

**Visual Example:**
```
Partition 1: ████████████████████████ (100GB) ← Skewed!
Partition 2: ██ (5GB)
Partition 3: ██ (5GB)
Partition 4: ██ (5GB)
```

### Symptoms of Data Skew

**1. In Spark UI:**
- One or few tasks take much longer than others
- Uneven task duration distribution
- High shuffle read for specific tasks
- Stragglers in stage execution

**2. In Logs:**
```
Task 1: 10 seconds
Task 2: 10 seconds
Task 3: 10 seconds
Task 4: 2 hours ← Skewed task!
```

**3. Performance Impact:**
- Overall job time = time of slowest task
- Resource waste (other executors idle)
- Potential OOM errors on skewed partitions

### Types of Data Skew

#### **1. Key Skew (Most Common)**
Uneven distribution of keys in group-by or join operations.

```python
# Example: User activity data
# user_id='bot_123' has 10M records
# Other users have ~100 records each

df.groupBy("user_id").count().show()
# +----------+--------+
# |   user_id|   count|
# +----------+--------+
# |   bot_123|10000000| ← Skewed!
# |   user_1 |     100|
# |   user_2 |      95|
```

#### **2. Partition Skew**
After shuffles, data lands unevenly in partitions.

```python
# Hash-based partitioning can create skew
# hash("popular_key") % num_partitions might cluster data
```

#### **3. Join Skew**
One side of join has skewed keys.

```python
# Large table with skewed keys joining small table
df_large.join(df_small, "key")
# If "key" has skewed distribution → slow join
```

### Detecting Data Skew

#### **Method 1: Check Spark UI**
```
1. Go to Stages tab
2. Click on a stage
3. Look at "Summary Metrics" - check:
   - Min, Median, Max task duration
   - If Max >> Median → Skew exists
```

#### **Method 2: Analyze Key Distribution**
```python
# Check distribution of keys
key_counts = df.groupBy("key").count() \
               .orderBy(col("count").desc())

key_counts.show()
# Identify keys with significantly higher counts

# Statistics
key_counts.describe("count").show()
```

#### **Method 3: Check Partition Sizes**
```python
# Check data distribution across partitions
partition_sizes = df.rdd.mapPartitions(
    lambda it: [sum(1 for _ in it)]
).collect()

print(f"Partition sizes: {partition_sizes}")
print(f"Max: {max(partition_sizes)}, Min: {min(partition_sizes)}")
```

### Solutions for Data Skew

#### **Solution 1: Salting (Most Effective)**

**What is Salting:**
Add a random "salt" value to skewed keys to distribute them across multiple partitions.

```python
from pyspark.sql.functions import rand, concat, lit

# Original skewed operation
result = df.groupBy("user_id").agg(sum("amount"))  # Skewed!

# Solution: Add salt
num_salts = 10

# Add salt to keys
df_salted = df.withColumn("salt", (rand() * num_salts).cast("int"))
df_salted = df_salted.withColumn(
    "salted_key",
    concat(col("user_id"), lit("_"), col("salt"))
)

# Group by salted key
result_salted = df_salted.groupBy("salted_key").agg(sum("amount"))

# Remove salt and aggregate again
result = result_salted.withColumn(
    "user_id",
    split(col("salted_key"), "_")[0]
).groupBy("user_id").agg(sum("sum(amount)"))
```

**For Joins:**
```python
# Salted join
num_salts = 10

# Salt the large table (one-to-one)
df_large_salted = df_large.withColumn(
    "salt",
    (rand() * num_salts).cast("int")
)

# Explode the small table (one-to-many)
from pyspark.sql.functions import explode, array

df_small_exploded = df_small.withColumn(
    "salt",
    explode(array([lit(i) for i in range(num_salts)]))
)

# Join on both key and salt
result = df_large_salted.join(
    df_small_exploded,
    ["key", "salt"]
).drop("salt")
```

#### **Solution 2: Adaptive Query Execution (AQE)**

**Enable AQE (Spark 3.0+):**
```python
# Automatically handles data skew
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# AQE will:
# 1. Detect skewed partitions during execution
# 2. Split large partitions
# 3. Reoptimize the plan
```

**AQE Configuration:**
```python
# Skew detection threshold
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")

# Partition size target
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
```

#### **Solution 3: Isolate Skewed Keys**

```python
# Separate skewed keys and process differently

# Identify skewed keys
skewed_keys = df.groupBy("user_id").count() \
                .filter(col("count") > 1000000) \
                .select("user_id")

# Broadcast skewed keys
skewed_keys_broadcast = broadcast(skewed_keys)

# Split data
df_skewed = df.join(skewed_keys_broadcast, "user_id", "inner")
df_normal = df.join(skewed_keys_broadcast, "user_id", "left_anti")

# Process separately
result_normal = df_normal.groupBy("user_id").agg(sum("amount"))
result_skewed = df_skewed.groupBy("user_id").agg(sum("amount"))

# Union results
result = result_normal.union(result_skewed)
```

#### **Solution 4: Increase Parallelism**

```python
# More partitions = smaller per-partition data
spark.conf.set("spark.sql.shuffle.partitions", "1000")  # Increase from 200

# Even better: Use AQE to auto-adjust
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

#### **Solution 5: Filter Skewed Data**

```python
# If skewed data is outliers/noise, filter it out

# Example: Remove bots/crawlers
df_filtered = df.filter(
    (col("user_agent").isNotNull()) &
    (~col("user_agent").like("%bot%")) &
    (~col("user_agent").like("%crawler%"))
)

result = df_filtered.groupBy("user_id").agg(sum("amount"))
```

#### **Solution 6: Custom Partitioning**

```python
# Manually partition by custom logic

# Define custom partitioner for RDD
def custom_partitioner(key):
    if key in skewed_keys:
        # Distribute skewed keys across many partitions
        return hash(key + str(random.random())) % 100
    else:
        return hash(key) % 10

rdd_partitioned = rdd.partitionBy(110, custom_partitioner)
```

#### **Solution 7: Broadcast Join for Skewed Side**

```python
# If one side is small, broadcast it
result = large_df_skewed.join(
    broadcast(small_df),
    "key"
)
# Avoids shuffle, eliminates skew issue
```

### Real-world Examples

#### **Example 1: E-commerce Order Analysis**

**Problem:**
```python
# Some popular products have millions of orders
# Most products have < 1000 orders

orders.groupBy("product_id").agg(
    sum("quantity"),
    avg("price")
)
# Task processing popular products takes hours
```

**Solution:**
```python
# Identify hot products
hot_products = orders.groupBy("product_id").count() \
                     .filter(col("count") > 100000)

# Process separately
hot_orders = orders.join(broadcast(hot_products), "product_id")
normal_orders = orders.join(
    broadcast(hot_products),
    "product_id",
    "left_anti"
)

# Aggregate with salting for hot products
hot_result = hot_orders \
    .withColumn("salt", (rand() * 20).cast("int")) \
    .groupBy("product_id", "salt") \
    .agg(sum("quantity"), avg("price")) \
    .groupBy("product_id") \
    .agg(
        sum("sum(quantity)").alias("total_quantity"),
        avg("avg(price)").alias("avg_price")
    )

# Normal aggregation for rest
normal_result = normal_orders.groupBy("product_id").agg(
    sum("quantity").alias("total_quantity"),
    avg("price").alias("avg_price")
)

# Combine
result = hot_result.union(normal_result)
```

#### **Example 2: User Session Analysis**

**Problem:**
```python
# Bot traffic creates skewed user_id distribution
sessions.groupBy("user_id").agg(count("session_id"))
# Bot users have 100K+ sessions, normal users < 100
```

**Solution:**
```python
# Enable AQE to handle automatically
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Or filter bots
sessions_filtered = sessions.filter(
    col("session_duration") > 1  # Bots have 0 duration
).filter(
    col("page_views") < 1000  # Bots have excessive views
)

result = sessions_filtered.groupBy("user_id").agg(
    count("session_id")
)
```

#### **Example 3: Skewed Join**

**Problem:**
```python
# User table: 100M users
# Events table: 10B events
# Some users (bots) have 10M events each

users.join(events, "user_id")
# Join takes forever due to skewed user_id
```

**Solution with Salting:**
```python
# Identify skewed users
skewed_users = events.groupBy("user_id").count() \
                     .filter(col("count") > 100000) \
                     .select("user_id")

# Split users
skewed_users_df = users.join(broadcast(skewed_users), "user_id")
normal_users_df = users.join(
    broadcast(skewed_users),
    "user_id",
    "left_anti"
)

# Normal join for normal users
normal_result = normal_users_df.join(events, "user_id")

# Salted join for skewed users
num_salts = 20

# Salt events (large table)
events_salted = events \
    .join(broadcast(skewed_users), "user_id") \
    .withColumn("salt", (rand() * num_salts).cast("int"))

# Explode users (small table)
skewed_users_exploded = skewed_users_df.withColumn(
    "salt",
    explode(array([lit(i) for i in range(num_salts)]))
)

# Join
skewed_result = events_salted.join(
    skewed_users_exploded,
    ["user_id", "salt"]
).drop("salt")

# Union
result = normal_result.union(skewed_result)
```

### Monitoring and Prevention

#### **1. Monitor Key Distribution**
```python
# Regularly check for skew
def check_skew(df, key_col):
    stats = df.groupBy(key_col).count()
    stats_summary = stats.describe("count")
    stats_summary.show()
    
    # Alert if max >> mean
    row = stats_summary.collect()
    max_val = float(row[7][1])  # max
    mean_val = float(row[1][1])  # mean
    
    if max_val > mean_val * 10:
        print(f"WARNING: Severe skew detected on {key_col}")
        return True
    return False
```

#### **2. Use Statistics**
```python
# Analyze statistics before operations
spark.sql("ANALYZE TABLE my_table COMPUTE STATISTICS")
spark.sql("ANALYZE TABLE my_table COMPUTE STATISTICS FOR COLUMNS key_col")
```

#### **3. Test with Samples**
```python
# Test on sample first
df_sample = df.sample(0.01)
result_sample = df_sample.groupBy("key").count()

# Check for skew before processing full dataset
check_skew(result_sample, "key")
```

### Best Practices

**1. Design Data to Avoid Skew**
- Avoid using null keys (nulls often cause skew)
- Design keys with good distribution
- Consider composite keys

**2. Enable AQE by Default**
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

**3. Monitor Proactively**
- Set up alerts for long-running tasks
- Check Spark UI regularly
- Profile data distribution

**4. Document Skewed Keys**
- Maintain list of known skewed keys
- Apply special handling in pipelines

**5. Choose Right Strategy**
- Small skew: Increase partitions
- Medium skew: Enable AQE
- Severe skew: Use salting or isolation

---

## 24. What is Z-Ordering and OPTIMIZE in Databricks?

### Z-Ordering

**Definition:** Z-ordering is a technique to colocate related information in the same set of files, improving query performance by reducing the amount of data scanned.

#### **How Z-Ordering Works**

**Traditional Layout:**
```
File 1: [user_id=1, date=2024-01-01], [user_id=5, date=2024-01-15]
File 2: [user_id=2, date=2024-01-05], [user_id=1, date=2024-01-10]
File 3: [user_id=3, date=2024-01-01], [user_id=1, date=2024-01-20]
```
Query: `WHERE user_id = 1` → Must scan all 3 files

**With Z-Ordering on user_id:**
```
File 1: [user_id=1, date=2024-01-01], [user_id=1, date=2024-01-10], [user_id=1, date=2024-01-20]
File 2: [user_id=2, date=2024-01-05]
File 3: [user_id=3, date=2024-01-01], [user_id=5, date=2024-01-15]
```
Query: `WHERE user_id = 1` → Only scans File 1

#### **Z-Ordering Syntax**

```sql
-- Using SQL
OPTIMIZE my_table
ZORDER BY (user_id, date)

-- With filter
OPTIMIZE my_table
WHERE date >= '2024-01-01'
ZORDER BY (user_id)
```

```python
# Using PySpark
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "/path/to/table")

# Z-order by single column
delta_table.optimize().executeZOrderBy("user_id")

# Z-order by multiple columns
delta_table.optimize().executeZOrderBy("user_id", "date")

# With condition
delta_table.optimize() \
    .where("date >= '2024-01-01'") \
    .executeZOrderBy("user_id")
```

#### **Z-Ordering vs Traditional Sorting**

| Aspect | Z-Ordering | Traditional Sorting |
|--------|-----------|-------------------|
| **Dimensions** | Multi-dimensional | Single dimension |
| **Data Locality** | Good for all columns | Good only for sort column |
| **Query Types** | Multiple column filters | Single column range queries |
| **Implementation** | Space-filling curve | Linear sort |

**Example:**
```python
# Traditional sort: Good for date queries only
df.repartition("date").sortWithinPartitions("date")

# Z-order: Good for both user_id AND date queries
OPTIMIZE table ZORDER BY (user_id, date)
```

### OPTIMIZE Command

**Definition:** OPTIMIZE command compacts small files into larger files, improving read performance and reducing metadata overhead.

#### **Why OPTIMIZE is Needed**

**Problem: Small Files**
```
After many INSERT/UPDATE/DELETE operations:
- Table has 10,000 small files (1-10MB each)
- Metadata overhead is high
- Query performance degrades
```

**Solution: OPTIMIZE**
```
After OPTIMIZE:
- Table has 100 larger files (1GB each)
- Less metadata overhead
- Faster queries
```

#### **OPTIMIZE Syntax**

```sql
-- Basic optimize
OPTIMIZE my_table

-- Optimize with Z-ordering
OPTIMIZE my_table
ZORDER BY (col1, col2)

-- Optimize specific partition
OPTIMIZE my_table
WHERE date = '2024-01-01'

-- Optimize with file size target
OPTIMIZE my_table
[ZORDER BY (col1)]
```

```python
# Using PySpark
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "/path/to/table")

# Basic optimize
delta_table.optimize().executeCompaction()

# Optimize with Z-order
delta_table.optimize().executeZOrderBy("user_id", "date")

# Optimize specific partition
delta_table.optimize() \
    .where("date = '2024-01-01'") \
    .executeCompaction()
```

#### **What OPTIMIZE Does**

1. **File Compaction:**
   - Reads small files
   - Combines them into larger files
   - Targets file size: 1GB (configurable)

2. **Removes Deleted Data:**
   - Physically removes deleted rows
   - Reclaims storage space

3. **Improves Data Layout:**
   - Colocates related data
   - Reduces file scanning

#### **OPTIMIZE Configuration**

```python
# Target file size (default: 1GB)
spark.conf.set("spark.databricks.delta.optimize.maxFileSize", "1073741824")

# Min file size to include in compaction (default: 128MB)
spark.conf.set("spark.databricks.delta.optimize.minFileSize", "134217728")

# Auto-optimize (automatically runs optimize)
spark.conf.set("spark.databricks.delta.autoOptimize.optimizeWrite", "true")
spark.conf.set("spark.databricks.delta.autoOptimize.autoCompact", "true")
```

### When to Use Z-Ordering

**Use Z-Order When:**

**1. Multiple Column Filters**
```sql
-- Queries filter on multiple columns
SELECT * FROM events
WHERE user_id = 123 AND date = '2024-01-15'

-- Z-order helps both filters
OPTIMIZE events ZORDER BY (user_id, date)
```

**2. High Cardinality Columns**
```python
# Good candidates: user_id, session_id, product_id
# Bad candidates: country (low cardinality), boolean flags

OPTIMIZE table ZORDER BY (user_id, product_id)
```

**3. Point Lookups and Range Queries**
```sql
-- Point lookup
SELECT * FROM orders WHERE order_id = '12345'

-- Range query
SELECT * FROM orders WHERE date BETWEEN '2024-01-01' AND '2024-01-31'

-- Z-order on both
OPTIMIZE orders ZORDER BY (order_id, date)
```

**Don't Use Z-Order When:**

**1. Single Column Queries**
```sql
-- If only querying by date, partition by date instead
-- Z-order adds overhead without benefit
```

**2. Low Cardinality Columns**
```python
# Bad: Only 2-10 unique values
OPTIMIZE table ZORDER BY (status)  # ❌

# Good: Millions of unique values
OPTIMIZE table ZORDER BY (user_id)  # ✅
```

**3. Frequently Changing Data**
```python
# Z-order is expensive
# Don't run on streaming tables with high frequency writes
```

### When to Use OPTIMIZE

**Use OPTIMIZE When:**

**1. Many Small Files**
```python
# Check file count
df = spark.read.format("delta").load("/path/to/table")
num_files = len(df.inputFiles())
print(f"Number of files: {num_files}")

# If > 1000 small files, run OPTIMIZE
if num_files > 1000:
    OPTIMIZE table
```

**2. After Bulk Operations**
```python
# After large INSERT/UPDATE/DELETE/MERGE
large_df.write.format("delta").mode("append").save("/path")

# Optimize to compact
OPTIMIZE table
```

**3. Query Performance Degradation**
```python
# If queries are slowing down
# Check if file count is high
# Run OPTIMIZE
```

**4. Before Important Queries**
```python
# Before critical analytical queries
OPTIMIZE analytics_table ZORDER BY (user_id, event_date)

# Then run queries
SELECT ...
```

### Best Practices

#### **1. Schedule Regular OPTIMIZE**
```python
# Run OPTIMIZE as a scheduled job (daily/weekly)

# Databricks notebook scheduled job
from delta.tables import DeltaTable

tables_to_optimize = ["orders", "events", "users"]

for table_name in tables_to_optimize:
    delta_table = DeltaTable.forName(spark, table_name)
    delta_table.optimize().executeCompaction()
    print(f"Optimized {table_name}")
```

#### **2. Choose Z-Order Columns Wisely**
```python
# Analyze query patterns
queries = [
    "WHERE user_id = ? AND date = ?",  # 60% of queries
    "WHERE product_id = ?",             # 30% of queries
    "WHERE category = ?"                # 10% of queries
]

# Z-order on most queried columns
OPTIMIZE table ZORDER BY (user_id, date, product_id)
# Max 3-4 columns for Z-order
```

#### **3. Optimize by Partition**
```python
# Don't optimize entire table if only recent data queried

# Optimize recent partitions only
OPTIMIZE table
WHERE date >= current_date() - INTERVAL 30 DAYS
ZORDER BY (user_id)
```

#### **4. Monitor OPTIMIZE Performance**
```sql
-- Check optimize history
DESCRIBE HISTORY my_table

-- Look for optimize operations
SELECT *
FROM (DESCRIBE HISTORY my_table)
WHERE operation = 'OPTIMIZE'
ORDER BY timestamp DESC
```

#### **5. Use Auto-Optimize**
```python
# For write-heavy tables
spark.conf.set("spark.databricks.delta.autoOptimize.optimizeWrite", "true")
spark.conf.set("spark.databricks.delta.autoOptimize.autoCompact", "true")

# Auto-optimize during write
df.write \
  .format("delta") \
  .option("optimizeWrite", "true") \
  .option("autoCompact", "true") \
  .save("/path/to/table")
```

### Real-world Examples

#### **Example 1: E-commerce Orders**
```python
# Table: orders
# Common queries:
# - Filter by order_date and customer_id
# - Filter by order_id

# Create table
orders_df.write.format("delta") \
    .partitionBy("year", "month") \
    .save("/mnt/data/orders")

# Optimize with Z-order
spark.sql("""
    OPTIMIZE delta.`/mnt/data/orders`
    WHERE year = 2024 AND month = 1
    ZORDER BY (customer_id, order_id)
""")

# Queries benefit:
spark.sql("""
    SELECT *
    FROM delta.`/mnt/data/orders`
    WHERE year = 2024 AND month = 1
      AND customer_id = 12345
""")  # Much faster due to Z-order
```

#### **Example 2: Clickstream Events**
```python
# High-volume streaming writes create many small files

# Check file count
events_df = spark.read.format("delta").load("/mnt/clickstream")
print(f"Files: {len(events_df.inputFiles())}")  # 5000 files!

# Optimize daily (scheduled job)
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "/mnt/clickstream")

# Optimize recent data with Z-order
delta_table.optimize() \
    .where("event_date >= current_date() - 7") \
    .executeZOrderBy("user_id", "session_id")

print(f"Files after: {len(events_df.inputFiles())}")  # 50 files!
```

#### **Example 3: Auto-Optimize for Streaming**
```python
# Streaming write with auto-optimize
streaming_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/checkpoints/events") \
    .option("optimizeWrite", "true") \
    .option("autoCompact", "true") \
    .table("events")

# Auto-optimize:
# - optimizeWrite: Reduces small files during write
# - autoCompact: Automatically compacts after threshold
```

### Performance Impact

**Before OPTIMIZE:**
```
Query time: 45 seconds
Files scanned: 5000
Data scanned: 100GB
```

**After OPTIMIZE:**
```
Query time: 8 seconds
Files scanned: 50
Data scanned: 100GB
```

**After OPTIMIZE + Z-ORDER:**
```
Query time: 2 seconds
Files scanned: 5
Data scanned: 10GB (data skipping)
```

### Cost Considerations

**OPTIMIZE Costs:**
- Reads data
- Rewrites data
- Compute time (minutes to hours for large tables)

**When to Run:**
- During off-peak hours
- After significant writes
- Before critical queries

**ROI:**
- Faster queries (10-100x speedup)
- Reduced query costs (less data scanned)
- Better user experience

---

## 25. What are StructType and StructField in Spark?

### StructType

**Definition:** `StructType` is a built-in class in PySpark that represents a schema (structure) of a DataFrame. It's a collection of `StructField` objects.

**Purpose:**
- Define DataFrame schema explicitly
- Ensure data types are correct
- Improve performance (avoids schema inference)
- Enable schema validation

#### **Basic Syntax**

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])

# Create DataFrame with schema
df = spark.read.schema(schema).csv("data.csv")
```

### StructField

**Definition:** `StructField` represents a single field (column) in a `StructType`. It defines:
- Column name
- Data type
- Nullable (True/False)
- Metadata (optional)

#### **StructField Parameters**

```python
StructField(name, dataType, nullable=True, metadata=None)
```

**Parameters:**
- `name`: Column name (string)
- `dataType`: Spark data type
- `nullable`: Whether null values are allowed (boolean)
- `metadata`: Additional metadata (dictionary)

### Available Data Types

```python
from pyspark.sql.types import *

# Numeric types
IntegerType()      # 32-bit signed integer
LongType()         # 64-bit signed integer
FloatType()        # 32-bit floating point
DoubleType()       # 64-bit floating point
DecimalType(10, 2) # Decimal with precision and scale

# String types
StringType()       # String
BinaryType()       # Binary data

# Boolean
BooleanType()      # True/False

# Date/Time
DateType()         # Date (year-month-day)
TimestampType()    # Timestamp (date + time)

# Complex types
ArrayType(StringType())              # Array of strings
MapType(StringType(), IntegerType()) # Map/Dictionary
StructType([...])                    # Nested structure
```

### Creating Schemas

#### **Method 1: Explicit StructType**

```python
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, DateType
)

# Define schema
user_schema = StructType([
    StructField("user_id", IntegerType(), False),  # NOT NULL
    StructField("username", StringType(), True),   # NULLABLE
    StructField("email", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", DoubleType(), True),
    StructField("join_date", DateType(), True)
])

# Create DataFrame
df = spark.read.schema(user_schema).csv("users.csv")
```

#### **Method 2: DDL String**

```python
# Define schema as DDL string
schema_ddl = "user_id INT, username STRING, email STRING, age INT"

df = spark.read.schema(schema_ddl).csv("users.csv")
```

#### **Method 3: From Existing DataFrame**

```python
# Get schema from existing DataFrame
existing_df = spark.read.csv("data.csv", header=True, inferSchema=True)
schema = existing_df.schema

# Use schema for new DataFrame
new_df = spark.read.schema(schema).csv("new_data.csv")
```

### Complex Data Types

#### **1. ArrayType**

```python
from pyspark.sql.types import ArrayType

# Array of strings
schema = StructType([
    StructField("name", StringType()),
    StructField("hobbies", ArrayType(StringType()), True)
])

# Data
data = [
    ("Alice", ["reading", "swimming"]),
    ("Bob", ["gaming", "cooking", "traveling"])
]

df = spark.createDataFrame(data, schema)
df.show()
# +-----+--------------------+
# | name|             hobbies|
# +-----+--------------------+
# |Alice|  [reading, swimming]|
# |  Bob|[gaming, cooking,...|
# +-----+--------------------+
```

#### **2. MapType**

```python
from pyspark.sql.types import MapType

# Map/Dictionary
schema = StructType([
    StructField("name", StringType()),
    StructField("scores", MapType(StringType(), IntegerType()), True)
])

# Data
data = [
    ("Alice", {"math": 95, "science": 88}),
    ("Bob", {"math": 78, "science": 92})
]

df = spark.createDataFrame(data, schema)
df.show(truncate=False)
# +-----+---------------------------+
# |name |scores                     |
# +-----+---------------------------+
# |Alice|{math -> 95, science -> 88}|
# |Bob  |{math -> 78, science -> 92}|
# +-----+---------------------------+
```

#### **3. Nested StructType**

```python
# Nested structure
address_schema = StructType([
    StructField("street", StringType()),
    StructField("city", StringType()),
    StructField("zipcode", StringType())
])

person_schema = StructType([
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("address", address_schema, True)  # Nested struct
])

# Data
data = [
    ("Alice", 30, ("123 Main St", "NYC", "10001")),
    ("Bob", 25, ("456 Oak Ave", "LA", "90001"))
]

df = spark.createDataFrame(data, person_schema)
df.show(truncate=False)
# +-----+---+--------------------------------+
# |name |age|address                         |
# +-----+---+--------------------------------+
# |Alice|30 |{123 Main St, NYC, 10001}       |
# |Bob  |25 |{456 Oak Ave, LA, 90001}        |
# +-----+---+--------------------------------+

# Access nested fields
df.select("name", "address.city").show()
# +-----+----+
# | name|city|
# +-----+----+
# |Alice| NYC|
# |  Bob|  LA|
# +-----+----+
```

### Why Use Explicit Schemas?

#### **1. Performance**

```python
# Without schema: Spark infers (slower)
df = spark.read.csv("large_file.csv", header=True, inferSchema=True)
# Spark reads data twice: once to infer schema, once to load

# With schema: No inference needed (faster)
df = spark.read.schema(schema).csv("large_file.csv")
# Spark reads data once
```

#### **2. Data Quality**

```python
# With schema: Type enforcement
schema = StructType([
    StructField("age", IntegerType(), False)  # Must be integer, NOT NULL
])

# Invalid data causes error
df = spark.createDataFrame([("abc",)], schema)  # Error! "abc" is not int
```

#### **3. Consistency**

```python
# Ensures consistent data types across reads
# Prevents "integer became string" issues
```

### Working with Schemas

#### **Print Schema**

```python
# Print schema structure
df.printSchema()

# Output:
# root
#  |-- name: string (nullable = true)
#  |-- age: integer (nullable = true)
#  |-- salary: double (nullable = true)
```

#### **Get Schema**

```python
# Get schema object
schema = df.schema
print(schema)

# Get as DDL string
schema_ddl = df.schema.simpleString()
print(schema_ddl)
# struct<name:string,age:int,salary:double>
```

#### **Modify Schema**

```python
# Add new field
from pyspark.sql.types import BooleanType

new_schema = StructType(df.schema.fields + [
    StructField("is_active", BooleanType(), True)
])

# Change field type
modified_schema = StructType([
    StructField(field.name, StringType() if field.dataType == IntegerType() else field.dataType, field.nullable)
    for field in df.schema.fields
])
```

### Real-world Examples

#### **Example 1: Reading JSON with Schema**

```python
# JSON data structure:
# {
#   "user_id": 1,
#   "profile": {
#     "name": "Alice",
#     "age": 30,
#     "email": "alice@example.com"
#   },
#   "purchase_history": [
#     {"product_id": "P1", "amount": 99.99},
#     {"product_id": "P2", "amount": 49.99}
#   ]
# }

# Define schema
purchase_schema = StructType([
    StructField("product_id", StringType()),
    StructField("amount", DoubleType())
])

profile_schema = StructType([
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("email", StringType())
])

user_schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("profile", profile_schema),
    StructField("purchase_history", ArrayType(purchase_schema))
])

# Read JSON with schema
df = spark.read.schema(user_schema).json("users.json")

# Query nested data
df.select(
    "user_id",
    "profile.name",
    "profile.email"
).show()

# Explode array
from pyspark.sql.functions import explode

df.select(
    "user_id",
    explode("purchase_history").alias("purchase")
).select(
    "user_id",
    "purchase.product_id",
    "purchase.amount"
).show()
```

#### **Example 2: Reading CSV with Schema**

```python
# CSV: large_transactions.csv (100GB)
# Columns: transaction_id, user_id, amount, date, status

# Define schema (avoid expensive inference)
transaction_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("user_id", IntegerType(), False),
    StructField("amount", DecimalType(10, 2), True),
    StructField("date", DateType(), True),
    StructField("status", StringType(), True)
])

# Read with schema
df = spark.read \
    .schema(transaction_schema) \
    .option("header", "true") \
    .csv("s3://bucket/large_transactions.csv")

# Much faster than inferSchema=True
```

#### **Example 3: Schema Evolution**

```python
# Old schema
old_schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType())
])

# New schema (added email column)
new_schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("email", StringType())
])

# Read old data with new schema (email will be null)
df_old = spark.read.schema(new_schema).csv("old_data.csv")

# Read new data
df_new = spark.read.schema(new_schema).csv("new_data.csv")

# Union works because schemas match
df_combined = df_old.union(df_new)
```

### Schema Validation

```python
# Validate schema before processing

def validate_schema(df, expected_schema):
    """Validate DataFrame schema matches expected schema"""
    if df.schema != expected_schema:
        print("Schema mismatch!")
        print("Expected:")
        expected_schema.printTreeString()
        print("Actual:")
        df.schema.printTreeString()
        return False
    return True

# Usage
expected_schema = StructType([...])
df = spark.read.csv("data.csv")

if validate_schema(df, expected_schema):
    # Process data
    result = df.groupBy("col1").sum("col2")
else:
    raise ValueError("Schema validation failed")
```

### Best Practices

**1. Always Define Schema for Production**
```python
# Good
df = spark.read.schema(schema).csv("data.csv")

# Bad (in production)
df = spark.read.csv("data.csv", inferSchema=True)
```

**2. Use Nullable Appropriately**
```python
# NOT NULL for required fields
StructField("user_id", IntegerType(), False)

# NULLABLE for optional fields
StructField("middle_name", StringType(), True)
```

**3. Document Schema**
```python
# Add metadata for documentation
schema = StructType([
    StructField("user_id", IntegerType(), False, 
                metadata={"description": "Unique user identifier"}),
    StructField("created_at", TimestampType(), False,
                metadata={"description": "Account creation timestamp"})
])
```

**4. Reuse Schemas**
```python
# Define once, use many times
USER_SCHEMA = StructType([...])

df1 = spark.read.schema(USER_SCHEMA).csv("users1.csv")
df2 = spark.read.schema(USER_SCHEMA).csv("users2.csv")
```

**5. Version Schemas**
```python
# Track schema versions
USER_SCHEMA_V1 = StructType([...])
USER_SCHEMA_V2 = StructType([...])  # Added new fields

# Use appropriate version
df = spark.read.schema(USER_SCHEMA_V2).parquet("data")
```

---

## 26. How do you read data from a CSV file in Spark?

### Basic CSV Reading

#### **Method 1: Using DataFrameReader**

```python
# Basic read
df = spark.read.csv("path/to/file.csv")

# With header
df = spark.read.csv("path/to/file.csv", header=True)

# With schema inference
df = spark.read.csv("path/to/file.csv", 
                    header=True, 
                    inferSchema=True)
```

#### **Method 2: Using Format Specification**

```python
# Explicit format
df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("path/to/file.csv")
```

#### **Method 3: Using options() Method**

```python
# Multiple options
df = spark.read \
    .options(header='true', inferSchema='true', delimiter=',') \
    .csv("path/to/file.csv")
```

### CSV Options

#### **Common Options**

```python
df = spark.read \
    .option("header", "true") \          # First row is header
    .option("inferSchema", "true") \     # Auto-detect data types
    .option("delimiter", ",") \          # Field delimiter (default: ,)
    .option("quote", '"') \              # Quote character (default: ")
    .option("escape", "\\") \            # Escape character (default: \)
    .option("mode", "PERMISSIVE") \      # Error handling mode
    .option("nullValue", "NULL") \       # String representing null
    .option("dateFormat", "yyyy-MM-dd") \ # Date format
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \ # Timestamp format
    .option("encoding", "UTF-8") \       # File encoding
    .option("ignoreLeadingWhiteSpace", "true") \
    .option("ignoreTrailingWhiteSpace", "true") \
    .option("multiLine", "true") \       # For multi-line records
    .option("comment", "#") \            # Comment character to skip lines
    .csv("path/to/file.csv")
```

### Reading with Schema

#### **Explicit Schema (Recommended for Production)**

```python
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, DateType
)

# Define schema
schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", DoubleType(), True),
    StructField("join_date", DateType(), True)
])

# Read with schema
df = spark.read \
    .schema(schema) \
    .option("header", "true") \
    .option("dateFormat", "yyyy-MM-dd") \
    .csv("users.csv")
```

**Benefits:**
- ✅ Faster (no schema inference)
- ✅ Type safety
- ✅ Consistent results
- ✅ Better error handling

### Reading Multiple Files

#### **Single Directory**

```python
# Read all CSV files in directory
df = spark.read.csv("path/to/directory/")

# With wildcard
df = spark.read.csv("path/to/directory/*.csv")
```

#### **Multiple Paths**

```python
# Read multiple specific files
df = spark.read.csv([
    "path/to/file1.csv",
    "path/to/file2.csv",
    "path/to/file3.csv"
])
```

#### **Pattern Matching**

```python
# Read files matching pattern
df = spark.read.csv("s3://bucket/data/year=2024/month=*/day=*/data.csv")

# Read partitioned data
df = spark.read.csv("s3://bucket/data/*/*/*.csv")
```

### Error Handling Modes

```python
# PERMISSIVE (default): Sets malformed records to null
df = spark.read \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .csv("data.csv")

# DROPMALFORMED: Drops malformed records
df = spark.read \
    .option("mode", "DROPMALFORMED") \
    .csv("data.csv")

# FAILFAST: Throws exception on malformed records
df = spark.read \
    .option("mode", "FAILFAST") \
    .csv("data.csv")
```

### Handling Special Cases

#### **Multi-line Records**

```python
# CSV with quoted multi-line fields
# Example: "Description: This is a
# multi-line description"

df = spark.read \
    .option("multiLine", "true") \
    .option("quote", '"') \
    .option("escape", '"') \
    .csv("data.csv")
```

#### **Custom Delimiters**

```python
# Tab-separated values
df = spark.read \
    .option("delimiter", "\t") \
    .csv("data.tsv")

# Pipe-separated values
df = spark.read \
    .option("delimiter", "|") \
    .csv("data.txt")

# Multiple character delimiter (use sep in Pandas-like way)
df = spark.read \
    .option("sep", "||") \
    .csv("data.txt")
```

#### **Compressed Files**

```python
# Spark automatically handles common compression formats

# Gzip
df = spark.read.csv("data.csv.gz")

# Bzip2
df = spark.read.csv("data.csv.bz2")

# Snappy (parquet default)
df = spark.read.csv("data.csv.snappy")

# No additional options needed
```

#### **Files with Comments**

```python
# Skip comment lines
df = spark.read \
    .option("comment", "#") \
    .csv("data.csv")

# Example file:
# # This is a comment
# id,name,age
# 1,Alice,30
# 2,Bob,25
```

### Real-world Examples

#### **Example 1: Production CSV Reading**

```python
from pyspark.sql.types import *

# Define schema explicitly
transaction_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("user_id", IntegerType(), False),
    StructField("amount", DecimalType(10, 2), True),
    StructField("transaction_date", DateType(), True),
    StructField("category", StringType(), True),
    StructField("description", StringType(), True),
    StructField("status", StringType(), True)
])

# Read with comprehensive options
df = spark.read \
    .schema(transaction_schema) \
    .option("header", "true") \
    .option("mode", "DROPMALFORMED") \
    .option("nullValue", "NULL") \
    .option("emptyValue", "") \
    .option("dateFormat", "yyyy-MM-dd") \
    .option("ignoreLeadingWhiteSpace", "true") \
    .option("ignoreTrailingWhiteSpace", "true") \
    .csv("s3://bucket/transactions/*.csv")

# Validate
print(f"Records loaded: {df.count()}")
df.printSchema()
df.show(5)
```

#### **Example 2: Handling Large Files**

```python
# For very large CSV files, optimize reading

# Read in parallel (multiple files)
df = spark.read \
    .option("header", "true") \
    .csv("s3://bucket/large-data/part-*.csv")

# Or partition single large file during read
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB

df = spark.read \
    .option("header", "true") \
    .csv("s3://bucket/very-large-file.csv")

# Check partitions
print(f"Number of partitions: {df.rdd.getNumPartitions()}")
```

#### **Example 3: Data Quality Checks**

```python
# Read CSV with corrupt record tracking
df = spark.read \
    .option("header", "true") \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .csv("data.csv")

# Check for corrupt records
corrupt_count = df.filter(col("_corrupt_record").isNotNull()).count()
print(f"Corrupt records: {corrupt_count}")

if corrupt_count > 0:
    print("Sample corrupt records:")
    df.filter(col("_corrupt_record").isNotNull()) \
      .select("_corrupt_record") \
      .show(10, truncate=False)

# Filter out corrupt records
df_clean = df.filter(col("_corrupt_record").isNull()) \
             .drop("_corrupt_record")
```

#### **Example 4: Reading with Type Coercion**

```python
# Sometimes data has inconsistent types
# Use schema to enforce types

schema = StructType([
    StructField("id", StringType()),  # Read as string first
    StructField("age", StringType()),  # Read as string
    StructField("salary", StringType())  # Read as string
])

df = spark.read \
    .schema(schema) \
    .option("header", "true") \
    .csv("messy_data.csv")

# Clean and cast
from pyspark.sql.functions import col, regexp_replace

df_clean = df \
    .withColumn("age", col("age").cast("integer")) \
    .withColumn("salary", 
                regexp_replace(col("salary"), "[$,]", "")
                .cast("double")
    ) \
    .filter(col("age").isNotNull())  # Filter invalid ages
```

#### **Example 5: Incremental CSV Reading**

```python
# Read new files only (batch processing)
from datetime import datetime, timedelta

# Get yesterday's date
yesterday = (datetime.now() - timedelta(1)).strftime("%Y-%m-%d")

# Read only yesterday's files
df = spark.read \
    .option("header", "true") \
    .csv(f"s3://bucket/data/date={yesterday}/*.csv")

# Or use modification time filter (requires listing files)
from pyspark import SparkFiles

def read_recent_csvs(path, hours=24):
    """Read CSV files modified in last N hours"""
    import os
    from datetime import datetime
    
    # List files
    files = spark.sparkContext.wholeTextFiles(path).keys().collect()
    
    # Filter by modification time
    cutoff = datetime.now().timestamp() - (hours * 3600)
    recent_files = [
        f for f in files
        if os.path.getmtime(f) > cutoff
    ]
    
    # Read recent files
    return spark.read.csv(recent_files, header=True)
```

### Performance Optimization

#### **1. Avoid inferSchema on Large Files**

```python
# Bad: Scans data twice
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \  # Expensive!
    .csv("large_file.csv")

# Good: Define schema explicitly
df = spark.read \
    .schema(my_schema) \
    .option("header", "true") \
    .csv("large_file.csv")
```

#### **2. Optimize Partition Size**

```python
# Control partition size
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB

# Or repartition after read
df = spark.read.csv("data.csv")
df = df.repartition(100)  # Distribute across 100 partitions
```

#### **3. Use Efficient File Formats (When Possible)**

```python
# If you control the data pipeline, convert to Parquet
df = spark.read.csv("data.csv", header=True)
df.write.parquet("data.parquet")

# Future reads are much faster
df = spark.read.parquet("data.parquet")
# Parquet: columnar, compressed, supports predicate pushdown
```

### Common Issues and Solutions

#### **Issue 1: Incorrect Schema Inference**

```python
# Problem: Number columns inferred as strings
df = spark.read.csv("data.csv", header=True, inferSchema=True)
df.printSchema()
# amount: string (should be double)

# Solution: Define schema explicitly
schema = StructType([
    StructField("amount", DoubleType())
])
df = spark.read.schema(schema).csv("data.csv", header=True)
```

#### **Issue 2: Quotes in Data**

```python
# Problem: Data contains quote characters
# Example: "Description "with quotes" inside"

# Solution: Set escape character
df = spark.read \
    .option("quote", '"') \
    .option("escape", '"') \  # Escape quotes with double quotes
    .csv("data.csv")
```

#### **Issue 3: Encoding Issues**

```python
# Problem: Non-UTF-8 encoding
# Error: "MalformedInputException"

# Solution: Specify encoding
df = spark.read \
    .option("encoding", "ISO-8859-1") \  # Or "windows-1252", etc.
    .csv("data.csv")
```

### Best Practices

**1. Always Use Schema in Production**
```python
# Development: OK to infer
df = spark.read.csv("data.csv", inferSchema=True)

# Production: Always explicit schema
df = spark.read.schema(schema).csv("data.csv")
```

**2. Handle Nulls Explicitly**
```python
df = spark.read \
    .option("nullValue", "NULL") \
    .option("emptyValue", "") \
    .csv("data.csv")
```

**3. Validate Data After Reading**
```python
# Check record count
print(f"Records: {df.count()}")

# Check for nulls
df.select([count(when(col(c).isNull(), c)).alias(c) 
           for c in df.columns]).show()

# Check schema
df.printSchema()
```

**4. Monitor Performance**
```python
# Check number of partitions
print(f"Partitions: {df.rdd.getNumPartitions()}")

# Check file sizes
print(f"Files: {len(df.inputFiles())}")
```

**5. Log Issues**
```python
import logging

logger = logging.getLogger(__name__)

try:
    df = spark.read.csv("data.csv")
    logger.info(f"Successfully loaded {df.count()} records")
except Exception as e:
    logger.error(f"Failed to read CSV: {str(e)}")
    raise
```

---

## 27. What is SparkContext and what is its role in Spark?

### SparkContext

**Definition:** SparkContext is the entry point to Spark functionality. It represents the connection to a Spark cluster and coordinates the execution of Spark applications.

#### **Key Characteristics**

```python
from pyspark import SparkContext, SparkConf

# Create SparkConf
conf = SparkConf().setAppName("MyApp").setMaster("local[*]")

# Create SparkContext
sc = SparkContext(conf=conf)

# In modern Spark (2.0+), use SparkSession instead
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .master("local[*]") \
    .getOrCreate()

# SparkContext is available through SparkSession
sc = spark.sparkContext
```

### Role of SparkContext

#### **1. Cluster Connection**
- Connects to cluster manager (YARN, Mesos, Kubernetes, Standalone)
- Requests resources (executors, memory, cores)
- Maintains connection throughout application lifetime

#### **2. RDD Creation**
```python
# Create RDD from collection
rdd = sc.parallelize([1, 2, 3, 4, 5])

# Create RDD from file
rdd = sc.textFile("hdfs://path/to/file.txt")

# Create RDD from multiple files
rdd = sc.wholeTextFiles("hdfs://path/to/directory")
```

#### **3. Job Scheduling and Execution**
- Schedules tasks on executors
- Tracks job progress
- Handles failures and retries

#### **4. Configuration Management**
```python
# Get configuration
conf = sc.getConf()
print(conf.get("spark.app.name"))
print(conf.get("spark.master"))

# Set configuration (before SparkContext creation)
sc.setLogLevel("ERROR")
```

#### **5. Broadcast Variables and Accumulators**
```python
# Broadcast variables (read-only shared data)
broadcast_var = sc.broadcast([1, 2, 3, 4, 5])
value = broadcast_var.value

# Accumulators (write-only shared variables)
accumulator = sc.accumulator(0)
rdd = sc.parallelize([1, 2, 3, 4, 5])
rdd.foreach(lambda x: accumulator.add(x))
print(accumulator.value)  # 15
```

### SparkContext vs SparkSession

| Aspect | SparkContext | SparkSession |
|--------|--------------|--------------|
| **Introduced** | Spark 1.x | Spark 2.0+ |
| **Purpose** | RDD operations | Unified entry point |
| **API** | Low-level RDD API | High-level DataFrame/Dataset API |
| **Usage** | Legacy applications | Modern applications |
| **Recommendation** | Use SparkSession.sparkContext | Use SparkSession directly |

```python
# Old way (Spark 1.x)
from pyspark import SparkContext
sc = SparkContext("local", "MyApp")
rdd = sc.textFile("data.txt")

# New way (Spark 2.0+)
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()
df = spark.read.text("data.txt")
# Access SparkContext if needed
sc = spark.sparkContext
```

### SparkContext Operations

```python
# Get Spark version
print(sc.version)

# Get application ID
print(sc.applicationId)

# Get master URL
print(sc.master)

# Get default parallelism
print(sc.defaultParallelism)

# Stop SparkContext
sc.stop()
```

---

## 28. You have millions of records with four states in a column where one state has very less data. How does partitioning work in this case?

### The Problem: Partition Skew

**Scenario:**
```
State  | Record Count
-------|-------------
CA     | 5,000,000  (50%)
TX     | 3,000,000  (30%)
NY     | 1,900,000  (19%)
WY     | 100,000    (1%)  ← Skewed!
```

When you partition by `state`, Wyoming partition has very little data compared to others.

### How Partitioning Works

#### **Physical Partitioning**
```python
# Write data partitioned by state
df.write.partitionBy("state").parquet("output")

# Creates directory structure:
# output/
#   state=CA/
#     part-00000.parquet (large file)
#     part-00001.parquet
#     ...
#   state=TX/
#     part-00000.parquet (large file)
#   state=NY/
#     part-00000.parquet (large file)
#   state=WY/
#     part-00000.parquet (tiny file) ← Problem!
```

### Issues with Skewed Partitioning

**1. Small Files Problem**
```python
# Wyoming partition creates tiny files
# - Inefficient storage
# - High metadata overhead
# - Slow queries (many file handles)
```

**2. Uneven Query Performance**
```python
# Query California data: Fast (large files, good parallelism)
df.filter(col("state") == "CA")

# Query Wyoming data: Slow (tiny files, poor parallelism)
df.filter(col("state") == "WY")
```

**3. Resource Waste**
```python
# Reading all states in parallel
# - CA partition: Uses many executors
# - WY partition: Uses one executor, wastes resources
```

### Solutions

#### **Solution 1: Combine Small Partitions with Larger Ones**

```python
from pyspark.sql.functions import when, col

# Create a composite partition key
df_partitioned = df.withColumn(
    "partition_key",
    when(col("state").isin("CA", "TX", "NY"), col("state"))
    .otherwise("OTHER")  # Group small states together
)

# Write with composite key
df_partitioned.write \
    .partitionBy("partition_key") \
    .parquet("output")

# Results in:
# state=CA/  (large)
# state=TX/  (large)
# state=NY/  (large)
# state=OTHER/  (WY + any other small states)
```

#### **Solution 2: Don't Partition by Skewed Column**

```python
# Instead of partitioning by state, use a different strategy

# Option A: Partition by date (if time-series data)
df.write.partitionBy("date").parquet("output")

# Option B: Partition by derived column with better distribution
df_with_region = df.withColumn(
    "region",
    when(col("state").isin("CA", "OR", "WA"), "West")
    .when(col("state").isin("TX", "OK", "AR"), "South")
    .when(col("state").isin("NY", "NJ", "PA"), "East")
    .otherwise("Other")
)

df_with_region.write.partitionBy("region").parquet("output")
```

#### **Solution 3: Use Bucketing Instead**

```python
# Bucket by state (fixed number of buckets)
df.write \
    .bucketBy(50, "state") \  # 50 buckets
    .sortBy("date") \
    .saveAsTable("state_data")

# All states distributed across 50 buckets
# - CA data: Spread across many buckets
# - WY data: Only in a few buckets
# - Better distribution
```

#### **Solution 4: Multi-level Partitioning**

```python
# Partition by date first (good distribution)
# Then by state (within each date)
df.write \
    .partitionBy("year", "month", "state") \
    .parquet("output")

# Directory structure:
# year=2024/
#   month=01/
#     state=CA/  (smaller, more manageable)
#     state=TX/
#     state=NY/
#     state=WY/  (still small, but better organized)
```

#### **Solution 5: Adaptive File Sizing**

```python
# Use file coalescing for small partitions
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Write data
df.write.partitionBy("state").parquet("output")

# For small states like WY, files will be coalesced
# For large states, files will be split appropriately
```

### Real-world Example

```python
# E-commerce data: USA states
# - CA, TX, NY: 80% of data
# - Other 47 states: 20% of data

from pyspark.sql.functions import col, when

# Solution: Tier-based partitioning
def create_state_tier(state_col):
    return when(state_col.isin("CA", "TX", "NY", "FL"), state_col) \
           .when(state_col.isin("PA", "IL", "OH", "GA"), "TIER2") \
           .otherwise("TIER3")

orders_df = orders_df.withColumn(
    "state_tier",
    create_state_tier(col("state"))
)

# Partition by date and state_tier
orders_df.write \
    .partitionBy("order_date", "state_tier") \
    .parquet("s3://bucket/orders")

# Result:
# order_date=2024-01-01/
#   state_tier=CA/     (large files)
#   state_tier=TX/     (large files)
#   state_tier=NY/     (large files)
#   state_tier=FL/     (large files)
#   state_tier=TIER2/  (medium files: PA, IL, OH, GA combined)
#   state_tier=TIER3/  (medium files: all other states combined)

# Queries still work with original state
# (store state in column, just partition by tier)
```

### Query Optimization with Skewed Partitions

```python
# Even with skewed partitions, optimize queries

# Bad: Query all states equally
df = spark.read.parquet("output")
df.filter(col("state") == "WY")  # Reads tiny partition, inefficient

# Good: Use partition pruning
df = spark.read.parquet("output/state=WY")  # Direct partition read

# Best: Combine with other filters
df = spark.read.parquet("output") \
    .filter((col("state") == "WY") & (col("date") >= "2024-01-01"))
```

### Monitoring Partition Sizes

```python
# Check partition sizes before writing
partition_stats = df.groupBy("state").count() \
    .orderBy(col("count").desc())

partition_stats.show()

# Identify skewed partitions
total_records = df.count()
partition_stats = partition_stats.withColumn(
    "percentage",
    (col("count") / total_records * 100)
)

# Alert on skewed partitions
skewed = partition_stats.filter(col("percentage") < 1)
print(f"Skewed partitions: {skewed.count()}")
skewed.show()
```

### Best Practices

**1. Analyze Distribution Before Partitioning**
```python
# Always check cardinality first
df.select("state").distinct().count()  # 50 states
df.groupBy("state").count().describe().show()  # Check distribution
```

**2. Choose Partition Columns Wisely**
- High cardinality with even distribution
- Columns used in WHERE clauses
- Avoid columns with skewed data

**3. Use Multi-level Partitioning**
```python
# Combine temporal and categorical partitioning
df.write.partitionBy("year", "month", "category").parquet("output")
```

**4. Set Target Partition Size**
```python
# Target: 128MB - 1GB per partition
# Avoid: < 10MB (too small) or > 5GB (too large)
```

**5. Consider Not Partitioning**
```python
# If all partitions would be small (< 128MB)
# Don't partition at all
df.write.parquet("output")  # Single directory

# Use bucketing or clustering instead
```

---

## 29. If you have one Spark action, how many stages and tasks will be created?

### Understanding Actions, Stages, and Tasks

#### **Basic Rule**
- **1 Action = 1 Job**
- **Stages = Depends on wide transformations (shuffles)**
- **Tasks = Number of partitions in each stage**

### Example 1: Simple Action (No Shuffle)

```python
# Single action, no wide transformations
df = spark.read.parquet("data.parquet")
df = df.filter(col("age") > 25)  # Narrow transformation
df = df.select("name", "age")     # Narrow transformation
count = df.count()                # Action

# Result:
# Jobs: 1 (from count action)
# Stages: 1 (no shuffles)
# Tasks: N (where N = number of partitions in df)
```

**Explanation:**
- All narrow transformations (filter, select) can be pipelined
- No shuffle boundaries = 1 stage
- Tasks = number of input partitions

### Example 2: Action with One Wide Transformation

```python
df = spark.read.parquet("data.parquet")  # Say 100 partitions
df = df.groupBy("department").sum("salary")  # Wide transformation (shuffle)
result = df.collect()  # Action

# Result:
# Jobs: 1 (from collect action)
# Stages: 2
#   - Stage 0: Read + partial aggregation (100 tasks, one per input partition)
#   - Stage 1: Final aggregation (200 tasks, spark.sql.shuffle.partitions=200)
# Total Tasks: 300
```

### Example 3: Action with Multiple Wide Transformations

```python
df = spark.read.parquet("data.parquet")  # 100 partitions
df = df.groupBy("dept").sum("salary")    # Wide transformation 1 (shuffle)
df = df.orderBy("sum(salary)")           # Wide transformation 2 (shuffle)
df.show()  # Action

# Result:
# Jobs: 1 (from show action)
# Stages: 3
#   - Stage 0: Read + partial aggregation (100 tasks)
#   - Stage 1: Final aggregation (200 tasks)
#   - Stage 2: Sort (200 tasks)
# Total Tasks: 500
```

### Stage Creation Rules

**Stage boundaries occur at:**
1. **Shuffle operations** (wide transformations)
   - groupBy, reduceByKey, join, repartition, coalesce, sortBy
2. **Actions**
   - Every action creates a new job
3. **Cached/Persisted RDDs**
   - Reading from cache can create stage boundary

```python
# Narrow transformations (no new stage):
- map, filter, flatMap
- select, withColumn
- union (if partitioned the same way)

# Wide transformations (create new stage):
- groupBy, agg, reduceByKey
- join, cogroup
- distinct, repartition
- sortBy, orderBy
```

### Task Creation Rules

**Tasks per stage:**
```
Tasks = Number of partitions processed in that stage
```

**Examples:**
```python
# Stage 1: Reading data (100 input partitions)
Tasks in Stage 1 = 100

# Stage 2: After shuffle (shuffle partitions = 200)
Tasks in Stage 2 = 200

# Stage 3: After another shuffle
Tasks in Stage 3 = 200
```

### Detailed Examples

#### **Example A: Multiple Actions**

```python
df = spark.read.parquet("data.parquet")
df = df.filter(col("active") == True)

# First action
count1 = df.count()  # Job 1, 1 stage, N tasks

# Second action (on same DataFrame)
count2 = df.groupBy("state").count().collect()  # Job 2, 2 stages, M tasks

# Total:
# Jobs: 2 (one per action)
# Stages: 3 (1 from first action, 2 from second action)
# Tasks: N + M (where M = N + shuffle.partitions)
```

#### **Example B: Cached Data**

```python
df = spark.read.parquet("data.parquet")  # 100 partitions
df = df.filter(col("age") > 25)
df.cache()  # Mark for caching

# First action materializes cache
df.count()  # Job 1, 1 stage, 100 tasks (fills cache)

# Second action reads from cache
df.select("name").show()  # Job 2, 1 stage, 100 tasks (from cache)

# Third action with shuffle
df.groupBy("state").count().show()  # Job 3, 2 stages
# - Stage 1: Read from cache (100 tasks)
# - Stage 2: Shuffle (200 tasks)
```

#### **Example C: Complex Pipeline**

```python
# Read data
df1 = spark.read.parquet("data1.parquet")  # 50 partitions
df2 = spark.read.parquet("data2.parquet")  # 100 partitions

# Transformations
df1_filtered = df1.filter(col("active") == True)  # Narrow
df2_grouped = df2.groupBy("user_id").sum("amount")  # Wide (shuffle)

# Join
result = df1_filtered.join(df2_grouped, "user_id")  # Wide (shuffle)

# Order
final = result.orderBy("amount")  # Wide (shuffle)

# Action
final.show()

# Analysis:
# Job: 1 (from show action)
# Stages: 5
#   Stage 0: Read df1 + filter (50 tasks)
#   Stage 1: Read df2 + partial aggregation (100 tasks)
#   Stage 2: Final aggregation (200 tasks, shuffle partitions)
#   Stage 3: Join (200 tasks)
#   Stage 4: Sort (200 tasks)
# Total Tasks: 750
```

### Calculating Tasks

**Formula:**
```
For each stage:
  If first stage (reading data):
    tasks = number of input partitions
  If shuffle stage:
    tasks = spark.sql.shuffle.partitions (default: 200)
```

**Example Calculation:**
```python
# Setup
spark.conf.set("spark.sql.shuffle.partitions", "100")
df = spark.read.parquet("data.parquet")  # Creates 50 partitions

# Operation
result = df.groupBy("key").count()  # Wide transformation
result.show()  # Action

# Stages and Tasks:
# Stage 0: Read + partial aggregation
#   Tasks = 50 (input partitions)
# Stage 1: Final aggregation
#   Tasks = 100 (shuffle partitions)
# Total: 150 tasks
```

### Monitoring in Spark UI

**Spark UI → Stages Tab:**
```
Stage 0: count at <console>:25
  - Tasks: 100
  - Shuffle Write: 2.5 GB

Stage 1: count at <console>:25
  - Tasks: 200
  - Shuffle Read: 2.5 GB
```

**Spark UI → Jobs Tab:**
```
Job 0: show at <console>:30
  - Stages: 3
  - Tasks: 500/500
  - Duration: 45 seconds
```

### Optimization

**Reduce Number of Stages:**
```python
# Bad: Multiple shuffles
df.repartition("col1") \
  .groupBy("col2").count() \
  .orderBy("count")
# 4 shuffles!

# Good: Combine operations
df.groupBy("col2").count() \
  .orderBy("count")
# 2 shuffles
```

**Optimize Task Count:**
```python
# Too few tasks (poor parallelism)
spark.conf.set("spark.sql.shuffle.partitions", "10")

# Too many tasks (high overhead)
spark.conf.set("spark.sql.shuffle.partitions", "10000")

# Optimal (based on data size and cluster)
# Target: 128MB - 1GB per partition
data_size_gb = 100
partition_size_gb = 0.5
optimal = data_size_gb / partition_size_gb
spark.conf.set("spark.sql.shuffle.partitions", str(int(optimal)))
```

### Key Takeaways

**1. Actions create jobs:**
- Each action = 1 job
- Multiple actions = multiple jobs

**2. Wide transformations create stages:**
- No wide transformations = 1 stage
- N wide transformations = N+1 stages

**3. Partitions determine tasks:**
- Input partitions for first stage
- Shuffle partitions for subsequent stages

**4. Formula:**
```
Jobs = Number of actions
Stages = 1 + Number of shuffles
Tasks per stage = Input partitions OR shuffle partitions
```

---

*End of Delta Lake / Storage Guide (Part 1)*

**Remaining Questions (30-42) to be covered:**
- 30-42: Delta Lake specifics (Delta Lake vs traditional, Delta format, ACID transactions, VACUUM, time travel, SCD Type 2, surrogate keys, ADLS Gen1 vs Gen2, Blob Storage vs ADLS Gen2)

Due to length constraints, these will be covered in a continuation or separate section.
