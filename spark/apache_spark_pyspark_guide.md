# Apache Spark / PySpark Interview Guide

## 1. What is the difference between a DataFrame and a Dataset in Spark? Which one do you prefer in real-time projects and why?

### DataFrame
- A distributed collection of data organized into named columns (like a table)
- Available in Python, Scala, Java, and R
- Uses untyped API (row-based operations)
- Optimized through Catalyst optimizer
- Runtime type safety - errors caught at runtime

### Dataset
- A strongly-typed, distributed collection of objects
- Only available in Scala and Java (not in Python or R)
- Uses typed API with compile-time type safety
- Also optimized through Catalyst optimizer
- Compile-time type safety - errors caught at compilation

### Real-time Project Preference
**DataFrames are generally preferred** because:
- Works across all languages (Python, Scala, Java, R)
- Better for structured data processing with SQL-like operations
- Less memory overhead compared to Datasets
- Sufficient for most use cases
- PySpark only supports DataFrames anyway

**Use Datasets when:**
- You need compile-time type safety (Scala/Java only)
- Working with complex custom objects
- Type-safe transformations are critical

---

## 2. What is a DAG in Spark?

**DAG (Directed Acyclic Graph)** is a logical execution plan that Spark creates for each job.

### Key Characteristics
- **Directed**: Flow of computation has a specific direction
- **Acyclic**: No cycles - transformations flow in one direction
- **Graph**: Nodes represent RDDs/DataFrames, edges represent transformations

### How It Works
1. When you call an action (like `collect()`, `count()`, `save()`), Spark creates a DAG
2. DAG Scheduler divides the DAG into stages based on wide transformations
3. Each stage contains tasks that can be executed in parallel
4. Tasks are scheduled on executors by the Task Scheduler

### Example
```python
# This creates a DAG but doesn't execute yet
df = spark.read.csv("data.csv")
df2 = df.filter(col("age") > 25)  # Transformation
df3 = df2.groupBy("city").count()  # Transformation

# Action triggers DAG execution
df3.show()
```

### Benefits
- Optimizes execution plan before running
- Identifies stages for parallel execution
- Enables fault tolerance through lineage tracking

---

## 3. What is a lineage graph and why is it important in Spark?

**Lineage graph** is a logical plan that tracks the sequence of transformations applied to create an RDD/DataFrame.

### Key Features
- Records all transformations from source to current state
- Maintains parent-child relationships between RDDs
- Immutable - once created, it doesn't change

### Importance

**1. Fault Tolerance**
- If a partition is lost, Spark can recompute it using lineage
- No need to checkpoint every intermediate result
- Only recomputes lost partitions, not entire dataset

**2. Lazy Evaluation**
- Transformations are recorded but not executed
- Allows optimization before execution

**3. Debugging**
- Can trace back how data was transformed
- Helps identify bottlenecks in the pipeline

### Example
```python
# Lineage is built with each transformation
rdd1 = sc.textFile("data.txt")  # Parent
rdd2 = rdd1.filter(lambda x: len(x) > 0)  # Child of rdd1
rdd3 = rdd2.map(lambda x: x.split(","))  # Child of rdd2

# View lineage
print(rdd3.toDebugString())
```

---

## 4. What are transformations in Spark? Explain narrow and wide transformations and how they impact Spark performance.

### Transformations
Operations that create a new RDD/DataFrame from an existing one. They are **lazy** - not executed until an action is called.

### Narrow Transformations
Each input partition contributes to **at most one output partition**.

**Examples:**
- `map()`, `filter()`, `flatMap()`
- `mapPartitions()`
- `union()`

**Characteristics:**
- No data shuffle across partitions
- Can be pipelined together
- Executed in the same stage
- Fast and efficient

```python
# Narrow transformation - no shuffle
df_filtered = df.filter(col("age") > 25)
df_mapped = df_filtered.select(col("name"), col("age") + 1)
```

### Wide Transformations
Each input partition contributes to **multiple output partitions**.

**Examples:**
- `groupBy()`, `reduceByKey()`
- `join()`, `repartition()`
- `distinct()`, `sortBy()`

**Characteristics:**
- Requires data shuffle across partitions
- Creates stage boundaries
- More expensive operations
- Network I/O intensive

```python
# Wide transformation - triggers shuffle
df_grouped = df.groupBy("department").agg(sum("salary"))
```

### Performance Impact

**Narrow Transformations:**
- ✅ Fast execution (in-memory)
- ✅ No network overhead
- ✅ Pipeline optimization possible

**Wide Transformations:**
- ⚠️ Slow due to shuffle
- ⚠️ Network I/O overhead
- ⚠️ Disk spilling if memory insufficient
- ⚠️ Creates stage boundaries (breaks pipeline)

**Optimization Tips:**
- Minimize wide transformations
- Use `reduceByKey()` instead of `groupByKey()`
- Filter data before joins
- Increase partition count for large shuffles
- Use broadcast joins for small tables

---

## 5. What happens internally once you submit a Spark job?

### Step-by-Step Execution Flow

**1. Job Submission**
- User submits application using `spark-submit`
- Driver program starts and creates SparkContext
- SparkContext connects to Cluster Manager (YARN/Mesos/Standalone/K8s)

**2. Resource Allocation**
- Cluster Manager allocates executors on worker nodes
- Executors register back with Driver
- Driver sends application code (JAR/Python files) to executors

**3. DAG Creation**
- When action is called, Driver creates a DAG of operations
- DAG Scheduler analyzes transformations and creates execution plan

**4. Stage Division**
- DAG Scheduler splits DAG into stages at shuffle boundaries
- Each stage contains tasks that can run in parallel
- Stages have dependencies (Stage 2 waits for Stage 1)

**5. Task Scheduling**
- Task Scheduler creates tasks for each partition in a stage
- Tasks are sent to executors based on data locality
- Task Scheduler monitors task execution

**6. Task Execution**
- Executors run tasks in parallel
- Results are stored in memory or disk
- Partial results sent back to Driver for actions like `collect()`

**7. Job Completion**
- All stages complete successfully
- Results returned to Driver
- Resources released back to Cluster Manager

### Architecture Components
```
Driver Program
├── SparkContext (entry point)
├── DAG Scheduler (creates stages)
├── Task Scheduler (schedules tasks)
└── Block Manager (manages data)

Cluster Manager (YARN/Mesos/K8s)
└── Resource allocation

Worker Nodes
└── Executors
    ├── Tasks (actual computation)
    └── Block Manager (cache management)
```

---

## 6. Explain Spark architecture, including Spark job, stage, and task, and explain the role of Driver, Executor, and Cluster Manager.

### Spark Architecture Components

#### **1. Driver**
The master process that coordinates the Spark application.

**Responsibilities:**
- Creates SparkContext
- Converts user program into tasks
- Schedules tasks on executors
- Maintains metadata about running application
- Serves UI for monitoring

**Location:** Runs on the client machine or cluster

#### **2. Executor**
Worker processes that run on cluster nodes.

**Responsibilities:**
- Execute tasks assigned by Driver
- Store data in memory or disk for caching
- Return results to Driver
- Each executor has fixed number of cores and memory

**Characteristics:**
- Multiple executors per application
- Lives for entire application lifetime
- Isolated from other applications

#### **3. Cluster Manager**
Manages resources across the cluster.

**Types:**
- **Standalone**: Spark's built-in cluster manager
- **YARN**: Hadoop's resource manager
- **Mesos**: Apache Mesos
- **Kubernetes**: Container orchestration

**Responsibilities:**
- Resource allocation (CPU, memory)
- Node management
- Application isolation

### Execution Hierarchy

#### **Job**
- Created when an action is called (`count()`, `collect()`, `save()`)
- One action = One job
- Can contain multiple stages

```python
df.filter(...).groupBy(...).count()  # Creates 1 job
```

#### **Stage**
- A set of tasks that can run in parallel
- Divided at shuffle boundaries (wide transformations)
- Stages have dependencies

```python
# Stage 1: Read + Filter (narrow)
df_filtered = df.filter(col("age") > 25)

# Stage 2: GroupBy (wide - creates new stage)
df_grouped = df_filtered.groupBy("city").count()
```

#### **Task**
- Smallest unit of work sent to an executor
- One task per partition
- Tasks in a stage run in parallel

```python
# If data has 100 partitions
# Stage will have 100 tasks (one per partition)
```

### Architecture Diagram Flow
```
User Program → Driver
                ↓
         [SparkContext created]
                ↓
         Cluster Manager ← [Request resources]
                ↓
         [Allocates Executors on Worker Nodes]
                ↓
         Driver → Executors [Sends tasks]
                ↓
         Executors → Driver [Returns results]
```

### Example Execution
```python
# Action triggers job
result = df.filter(col("age") > 25) \  # Stage 1 (narrow)
           .groupBy("city") \           # Stage 2 (wide)
           .count() \                   # Stage 2
           .collect()                   # Action - triggers execution

# Creates:
# - 1 Job (from collect action)
# - 2 Stages (shuffle at groupBy)
# - Multiple Tasks per stage (1 per partition)
```

---

## 7. If a Spark job has two wide transformations and one action on 2GB of data, how many jobs, stages, and tasks will be created?

### Given
- 2 wide transformations
- 1 action
- 2GB of data

### Answer

#### **Jobs: 1**
- Only actions create jobs
- 1 action = 1 job

#### **Stages: 3**
- Initial stage (before first wide transformation)
- Stage after first wide transformation
- Stage after second wide transformation
- Wide transformations create stage boundaries

#### **Tasks: Depends on number of partitions**

**Task count per stage:**
```
Tasks = Number of partitions in that stage
```

**Default partition calculation:**
```
Partitions = max(spark.default.parallelism, 
                 data_size / spark.sql.files.maxPartitionBytes)

# Default spark.sql.files.maxPartitionBytes = 128MB
Partitions for 2GB ≈ 2048MB / 128MB = 16 partitions
```

**Total tasks:**
- **Stage 1**: ~16 tasks (one per partition)
- **Stage 2**: Depends on shuffle partitions (default 200)
  - Configurable via `spark.sql.shuffle.partitions`
- **Stage 3**: Depends on shuffle partitions (default 200)

**Typical scenario:**
- Stage 1: 16 tasks
- Stage 2: 200 tasks (default shuffle partitions)
- Stage 3: 200 tasks
- **Total: ~416 tasks**

### Visual Example
```python
df = spark.read.csv("2GB_data.csv")  # Creates initial RDD

# Stage 1: Read + narrow transformations
df_filtered = df.filter(col("age") > 25)

# Stage 2: First wide transformation (shuffle)
df_grouped = df_filtered.groupBy("department").sum("salary")

# Stage 3: Second wide transformation (shuffle)
df_sorted = df_grouped.orderBy("sum(salary)")

# Action: Triggers the job with 3 stages
df_sorted.show()  # 1 Job, 3 Stages, ~416 Tasks
```

### Configuration Impact
```python
# Reduce shuffle partitions to optimize
spark.conf.set("spark.sql.shuffle.partitions", "16")

# Now:
# Stage 1: 16 tasks
# Stage 2: 16 tasks
# Stage 3: 16 tasks
# Total: 48 tasks
```

---

## 8. What is a partition in Spark and based on what factors are Spark partitions created?

### What is a Partition?

A **partition** is a logical chunk of data that can be processed independently on a single executor core.

**Key characteristics:**
- Smallest unit of parallelism
- One task processes one partition
- Stored in-memory or on disk
- Cannot be split across multiple executors

### Factors That Determine Partitions

#### **1. Input Data Source**

**For Files (HDFS, S3, Local):**
```python
# Partition size controlled by:
spark.sql.files.maxPartitionBytes = 128MB (default)

# Example: 1GB file
Partitions = 1024MB / 128MB = 8 partitions
```

**For JDBC/Database:**
```python
df = spark.read.jdbc(url, table,
    numPartitions=10,  # Explicit partition count
    column="id",       # Partition column
    lowerBound=1,      # Minimum value
    upperBound=1000)   # Maximum value
```

#### **2. Parallelism Configuration**
```python
# Default parallelism
spark.default.parallelism = Total_Cores_in_Cluster

# For RDD operations
rdd = sc.parallelize(data, numSlices=10)
```

#### **3. Shuffle Operations**
```python
# Shuffle partitions (for wide transformations)
spark.sql.shuffle.partitions = 200 (default)

# After groupBy, join, reduceByKey
df_grouped = df.groupBy("city").count()
# Creates 200 partitions (by default)
```

#### **4. Explicit Repartitioning**
```python
# Increase partitions (triggers shuffle)
df_repartitioned = df.repartition(100)

# Decrease partitions (no shuffle, coalesce)
df_coalesced = df.coalesce(10)
```

#### **5. Transformation Type**

**Narrow transformations:** Maintain partition count
```python
df_filtered = df.filter(col("age") > 25)
# Same number of partitions as df
```

**Wide transformations:** Create new partitions based on shuffle config
```python
df_grouped = df.groupBy("city").count()
# Creates spark.sql.shuffle.partitions (default 200)
```

### Factors to Consider for Optimal Partitioning

#### **Too Few Partitions**
- ❌ Underutilization of cluster resources
- ❌ Slower processing
- ❌ Risk of OOM errors

#### **Too Many Partitions**
- ❌ High scheduling overhead
- ❌ More task serialization overhead
- ❌ Slower execution

#### **Optimal Partition Count**
```
Ideal partitions = 2-4 × Number of total cores in cluster

Partition size = 128MB to 1GB per partition
```

### Example Calculations
```python
# Cluster: 5 nodes, 4 cores each = 20 total cores
# Data size: 10GB

# Recommended:
# Partitions = 2-4 × 20 = 40-80 partitions
# Partition size = 10GB / 60 = ~170MB per partition ✅

# Configure:
spark.conf.set("spark.sql.shuffle.partitions", "60")
```

---

## 9. What is the data locality principle in Spark and why is it important?

### Data Locality

**Data locality** is the principle of moving computation to where data resides, rather than moving data to computation.

### Levels of Data Locality (Best to Worst)

#### **1. PROCESS_LOCAL** (Best)
- Data is in the same JVM process as the task
- Data is cached in executor memory
- Fastest - no data movement

```python
df.cache()  # Caches data in executor memory
df.count()  # PROCESS_LOCAL access
```

#### **2. NODE_LOCAL**
- Data is on the same node but different process
- Might be on local disk or another executor
- Still fast - local disk/memory access

#### **3. RACK_LOCAL**
- Data is on a different node but same rack
- Network transfer within rack
- Moderate speed

#### **4. ANY** (Worst)
- Data is on a different rack
- Network transfer across racks
- Slowest - maximum data movement

### Why Data Locality is Important

**1. Performance**
- Reduces network I/O
- Minimizes data transfer time
- Maximizes throughput

**2. Resource Efficiency**
- Less network bandwidth usage
- Reduces cluster congestion
- Better resource utilization

**3. Cost**
- Lower network costs in cloud
- Reduced execution time = lower compute costs

### How Spark Achieves Data Locality

**1. Preferred Locations**
- Spark tracks where data blocks are stored
- Schedules tasks on nodes with data

**2. Delay Scheduling**
- Waits for a preferred node to become available
- Configurable wait time before compromising locality

```python
# Locality wait time (default: 3s)
spark.locality.wait = 3s
spark.locality.wait.node = 3s
spark.locality.wait.rack = 3s
```

**3. Data Partitioning**
- Co-locates related data
- Reduces shuffles

### Real-world Example

```python
# Bad: Data not co-located
df1 = spark.read.csv("hdfs://cluster1/data1.csv")
df2 = spark.read.csv("hdfs://cluster2/data2.csv")
result = df1.join(df2, "id")  # Heavy data movement

# Good: Co-locate data
df1 = spark.read.csv("hdfs://cluster1/data1.csv")
df2 = spark.read.csv("hdfs://cluster1/data2.csv")
result = df1.join(df2, "id")  # Minimal data movement
```

### Optimization Tips

**1. Cache frequently used data**
```python
df.cache()  # Ensures PROCESS_LOCAL access
```

**2. Use broadcast joins for small tables**
```python
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "id")
```

**3. Partition data appropriately**
```python
df.write.partitionBy("date").parquet("output")
```

**4. Monitor locality in Spark UI**
- Check "Locality Level" in Stages tab
- Aim for PROCESS_LOCAL or NODE_LOCAL

---

## 10. What is the difference between client mode and cluster mode? What real-time challenges have you faced using them?

### Client Mode vs Cluster Mode

| Aspect | Client Mode | Cluster Mode |
|--------|-------------|--------------|
| **Driver Location** | On client machine | On cluster node |
| **Driver Failure** | Application fails | Can be restarted by cluster manager |
| **Network** | Driver-executor communication over network | Driver and executors on same network |
| **Use Case** | Interactive, development, notebooks | Production, long-running jobs |
| **Spark Submit** | `--deploy-mode client` | `--deploy-mode cluster` |

### Client Mode

**How it works:**
```
Client Machine (Driver) → Cluster Manager → Executors
```

**Characteristics:**
- Driver runs on the machine where spark-submit is run
- Driver communicates with executors over network
- Console output visible immediately
- Useful for interactive shells (spark-shell, pyspark, notebooks)

**Advantages:**
- ✅ Easy debugging (logs on client)
- ✅ Immediate feedback
- ✅ Good for development/testing

**Disadvantages:**
- ❌ Client must stay alive for entire job
- ❌ Network latency between driver and executors
- ❌ Not suitable for long-running jobs
- ❌ Firewall issues possible

### Cluster Mode

**How it works:**
```
Client Machine → Cluster Manager → Launches Driver on Cluster → Executors
```

**Characteristics:**
- Driver runs on one of the cluster nodes
- Client can disconnect after submission
- Driver and executors on same network
- Better for production jobs

**Advantages:**
- ✅ No dependency on client machine
- ✅ Better network performance
- ✅ Driver can be restarted on failure (YARN)
- ✅ Client can disconnect

**Disadvantages:**
- ❌ Logs not immediately visible
- ❌ Harder to debug
- ❌ Need to access cluster logs

### Real-time Challenges

#### **Challenge 1: Client Mode Network Issues**
**Problem:**
- Driver on local machine, executors in cloud
- High network latency for large collect operations
- Timeout errors when collecting large results

**Solution:**
```python
# Avoid collecting large results
df.count()  # ✅ Returns single value
df.collect()  # ❌ Brings all data to driver

# Use cluster mode for production
# Or save results to storage
df.write.parquet("s3://bucket/output")
```

#### **Challenge 2: Client Disconnection**
**Problem:**
- Long-running job in client mode
- Client machine lost connection/crashed
- Entire job failed

**Solution:**
```bash
# Use cluster mode for long jobs
spark-submit --deploy-mode cluster \
  --master yarn \
  my_script.py
```

#### **Challenge 3: Firewall Restrictions**
**Problem:**
- Corporate firewall blocks driver-executor communication
- Client mode fails with connection errors

**Solution:**
- Use cluster mode (all communication within cluster)
- Or configure firewall rules for required ports

#### **Challenge 4: Memory Issues with Large Collects**
**Problem:**
```python
# Client mode - driver on machine with 8GB RAM
large_df = spark.read.parquet("10GB_data")
result = large_df.collect()  # OOM on client!
```

**Solution:**
```python
# Don't collect large datasets
large_df.write.parquet("output")  # Write to storage
# Or use cluster mode with adequate driver memory
```

#### **Challenge 5: Debugging Cluster Mode**
**Problem:**
- Logs scattered across cluster nodes
- Hard to trace errors

**Solution:**
```bash
# Use YARN logs
yarn logs -applicationId <app_id>

# Or check Spark UI
# http://<spark-master>:4040
```

### Best Practices

**Use Client Mode For:**
- Interactive development (notebooks, spark-shell)
- Quick testing and debugging
- Jobs with small result sets
- When you need immediate console output

**Use Cluster Mode For:**
- Production jobs
- Long-running applications
- Jobs that collect large results
- When reliability is critical
- Scheduled/automated jobs

---

## 11. What is on-heap and off-heap memory in Spark and how does Spark manage memory internally?

### Heap vs Off-Heap Memory

#### **On-Heap Memory**
- Managed by JVM (Java Virtual Machine)
- Subject to garbage collection (GC)
- Default memory type in Spark
- Located in Java heap space

#### **Off-Heap Memory**
- Managed by Spark directly (not JVM)
- Not subject to GC
- Stored in native memory (outside JVM)
- Requires explicit enabling

### Spark Memory Management Architecture

#### **Executor Memory Structure**
```
Total Executor Memory
│
├── Reserved Memory (300MB fixed)
│
├── Spark Memory (spark.memory.fraction = 0.6 default)
│   ├── Storage Memory (50% by default)
│   │   └── Caching, broadcast variables
│   └── Execution Memory (50% by default)
│       └── Joins, aggregations, shuffles
│
└── User Memory (0.4 default)
    └── User code, data structures
```

### Memory Regions in Detail

#### **1. Reserved Memory: 300MB**
- Fixed size for Spark internal objects
- Cannot be configured

#### **2. Spark Memory (60% of total)**
```python
spark.memory.fraction = 0.6  # 60% for Spark operations
```

**Storage Memory:**
- Caching DataFrames/RDDs (`cache()`, `persist()`)
- Broadcast variables
- Can borrow from Execution Memory if available

**Execution Memory:**
- Shuffle operations
- Joins, aggregations, sorts
- Can borrow from Storage Memory
- Evicts cached data if needed (LRU)

**Memory Borrowing:**
- Storage can borrow from unused Execution memory
- Execution can evict cached data from Storage
- Controlled by `spark.memory.storageFraction = 0.5`

#### **3. User Memory (40%)**
```python
# Remaining 40% for:
# - User data structures
# - UDFs
# - Spark internal metadata
```

### On-Heap Configuration
```python
# Set executor memory
spark-submit --executor-memory 10G

# Memory split (default):
# Reserved: 300MB
# Spark Memory: (10G - 300MB) × 0.6 = 5.82G
#   ├── Storage: 2.91G
#   └── Execution: 2.91G
# User Memory: (10G - 300MB) × 0.4 = 3.88G
```

### Off-Heap Memory

#### **Enabling Off-Heap**
```python
spark.memory.offHeap.enabled = true
spark.memory.offHeap.size = 5G  # Additional memory
```

#### **Benefits of Off-Heap**
- ✅ No GC overhead
- ✅ More predictable performance
- ✅ Better for large caching
- ✅ Reduces GC pauses

#### **Use Cases**
- Large cached datasets
- Shuffle-heavy workloads
- Applications sensitive to GC pauses

#### **Memory Calculation with Off-Heap**
```
Total Memory = On-Heap + Off-Heap

On-Heap: 10G
Off-Heap: 5G
Total Available: 15G
```

### Unified Memory Management (Spark 1.6+)

**Dynamic allocation between Storage and Execution:**
```
┌─────────────────────────────────┐
│     Spark Memory (60%)          │
├─────────────────────────────────┤
│  Storage ←→ Execution           │
│  (Dynamic boundary)             │
│                                 │
│  Both can borrow from each      │
│  other when idle                │
└─────────────────────────────────┘
```

**Rules:**
1. Storage can borrow unused Execution memory
2. Execution can evict cached blocks from Storage
3. Storage cannot evict Execution memory
4. Execution always has priority

### Memory Management Example

```python
# Initial state
Storage: 3GB | Execution: 3GB

# Scenario 1: Heavy caching
df.cache()
# Storage: 5GB (borrowed 2GB) | Execution: 1GB

# Scenario 2: Then a shuffle operation
df_grouped = df.groupBy("city").count()
# Execution needs memory, evicts cached blocks
# Storage: 2GB (evicted 3GB) | Execution: 4GB
```

### Common Memory Issues and Solutions

#### **Problem 1: OutOfMemoryError**
```python
# Too much data cached
df.cache()
df.count()  # OOM!
```

**Solution:**
```python
# Increase executor memory
--executor-memory 20G

# Or use disk-based persistence
df.persist(StorageLevel.MEMORY_AND_DISK)

# Or don't cache if not reused
df.count()  # No cache
```

#### **Problem 2: GC Overhead**
```python
# Symptom: Tasks slow with frequent GC
```

**Solution:**
```python
# Enable off-heap memory
spark.memory.offHeap.enabled = true
spark.memory.offHeap.size = 10G

# Increase memory fraction
spark.memory.fraction = 0.7
```

#### **Problem 3: Shuffle Spill to Disk**
```python
# Symptom: Shuffle spill (memory) and (disk) in UI
```

**Solution:**
```python
# Increase executor memory
--executor-memory 16G

# Increase shuffle partitions
spark.sql.shuffle.partitions = 400

# Increase execution memory fraction
spark.memory.fraction = 0.7
```

### Best Practices

**1. Right-size executor memory**
```bash
# Don't exceed 40-50GB per executor
# More executors with less memory each is better
--executor-memory 32G  # ✅
--executor-memory 100G # ❌ (GC issues)
```

**2. Monitor memory usage**
- Check Spark UI → Storage tab
- Check Executors tab for memory usage
- Look for spill metrics

**3. Use off-heap for large datasets**
```python
spark.memory.offHeap.enabled = true
spark.memory.offHeap.size = 10G
```

**4. Avoid caching everything**
```python
# Only cache if reused
if df.rdd.getNumPartitions() > 0:
    df.cache()
```

**5. Use appropriate persistence levels**
```python
from pyspark import StorageLevel

# Memory only (default)
df.persist(StorageLevel.MEMORY_ONLY)

# Spill to disk if memory full
df.persist(StorageLevel.MEMORY_AND_DISK)

# Serialized (saves memory)
df.persist(StorageLevel.MEMORY_ONLY_SER)
```

---

## 12. What transformations have you commonly used in PySpark?

### Most Commonly Used PySpark Transformations

#### **1. filter() / where()**
Filter rows based on condition
```python
# Filter rows where age > 25
df_filtered = df.filter(col("age") > 25)
# OR
df_filtered = df.where(col("age") > 25)

# Multiple conditions
df_filtered = df.filter((col("age") > 25) & (col("city") == "NYC"))
```

#### **2. select()**
Select specific columns
```python
# Select columns
df_selected = df.select("name", "age", "city")

# With transformations
df_selected = df.select(
    col("name"),
    (col("age") + 1).alias("age_next_year"),
    col("salary") * 1.1
)
```

#### **3. withColumn()**
Add or modify columns
```python
# Add new column
df_new = df.withColumn("age_category",
    when(col("age") < 18, "Minor")
    .when(col("age") < 65, "Adult")
    .otherwise("Senior")
)

# Modify existing column
df_new = df.withColumn("salary", col("salary") * 1.1)

# Multiple columns
df_new = df.withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name"))) \
           .withColumn("age_squared", col("age") ** 2)
```

#### **4. groupBy() + agg()**
Group data and aggregate
```python
from pyspark.sql.functions import sum, avg, count, max, min

# Group by single column
df_grouped = df.groupBy("department").agg(
    sum("salary").alias("total_salary"),
    avg("salary").alias("avg_salary"),
    count("*").alias("employee_count")
)

# Group by multiple columns
df_grouped = df.groupBy("department", "city").agg(
    max("salary").alias("max_salary")
)
```

#### **5. join()**
Join DataFrames
```python
# Inner join (default)
df_joined = df1.join(df2, on="employee_id", how="inner")

# Left outer join
df_joined = df1.join(df2, on="employee_id", how="left")

# Multiple join conditions
df_joined = df1.join(df2, 
    (df1.emp_id == df2.emp_id) & (df1.dept == df2.dept),
    how="inner"
)

# Join types: inner, left, right, full, left_semi, left_anti, cross
```

#### **6. orderBy() / sort()**
Sort data
```python
# Ascending
df_sorted = df.orderBy("age")

# Descending
df_sorted = df.orderBy(col("salary").desc())

# Multiple columns
df_sorted = df.orderBy(col("department").asc(), col("salary").desc())
```

#### **7. drop() / dropDuplicates()**
Remove columns or duplicates
```python
# Drop columns
df_dropped = df.drop("column1", "column2")

# Remove duplicates
df_unique = df.dropDuplicates()

# Remove duplicates based on specific columns
df_unique = df.dropDuplicates(["employee_id"])
```

#### **8. fillna() / dropna()**
Handle null values
```python
# Fill nulls with specific values
df_filled = df.fillna({"age": 0, "city": "Unknown"})

# Drop rows with any null
df_dropped = df.dropna()

# Drop rows with nulls in specific columns
df_dropped = df.dropna(subset=["age", "salary"])

# Drop rows with all nulls
df_dropped = df.dropna(how="all")
```

#### **9. withColumnRenamed()**
Rename columns
```python
# Rename single column
df_renamed = df.withColumnRenamed("old_name", "new_name")

# Rename multiple columns (chain or use select)
df_renamed = df.select(
    col("emp_id").alias("employee_id"),
    col("dept").alias("department")
)
```

#### **10. union() / unionByName()**
Combine DataFrames vertically
```python
# Union (requires same schema)
df_combined = df1.union(df2)

# Union by column names (handles different order)
df_combined = df1.unionByName(df2)
```

#### **11. distinct()**
Get unique rows
```python
df_unique = df.distinct()

# Unique values in specific column
unique_cities = df.select("city").distinct()
```

#### **12. when() / otherwise()**
Conditional logic
```python
from pyspark.sql.functions import when

df_new = df.withColumn("age_group",
    when(col("age") < 18, "Child")
    .when((col("age") >= 18) & (col("age") < 65), "Adult")
    .otherwise("Senior")
)
```

#### **13. cast()**
Change column data types
```python
# Cast to different types
df_casted = df.withColumn("age", col("age").cast("integer"))
df_casted = df.withColumn("salary", col("salary").cast("double"))
df_casted = df.withColumn("date", col("date").cast("date"))
```

#### **14. explode()**
Convert array/map columns to rows
```python
from pyspark.sql.functions import explode

# Explode array column
df_exploded = df.withColumn("tag", explode(col("tags_array")))

# Input:  [{"name": "John", "tags": ["python", "spark"]}]
# Output: [{"name": "John", "tag": "python"},
#          {"name": "John", "tag": "spark"}]
```

#### **15. pivot()**
Convert rows to columns
```python
# Pivot table
df_pivoted = df.groupBy("year").pivot("department").agg(sum("salary"))

# Input:  year | dept  | salary
#         2023 | Sales | 50000
#         2023 | IT    | 60000
# Output: year | Sales | IT
#         2023 | 50000 | 60000
```

### String Functions

```python
from pyspark.sql.functions import (
    lower, upper, trim, ltrim, rtrim,
    substring, concat, concat_ws,
    regexp_replace, split
)

# String operations
df_string = df.withColumn("name_lower", lower(col("name"))) \
              .withColumn("name_upper", upper(col("name"))) \
              .withColumn("name_trimmed", trim(col("name"))) \
              .withColumn("first_name", split(col("full_name"), " ")[0])
```

### Date Functions

```python
from pyspark.sql.functions import (
    current_date, current_timestamp,
    date_format, to_date, to_timestamp,
    year, month, dayofmonth, datediff
)

# Date operations
df_dates = df.withColumn("current_date", current_date()) \
             .withColumn("year", year(col("date"))) \
             .withColumn("month", month(col("date"))) \
             .withColumn("days_diff", datediff(current_date(), col("date")))
```

### Window Functions

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, lag, lead

# Define window
window_spec = Window.partitionBy("department").orderBy(col("salary").desc())

# Apply window functions
df_windowed = df.withColumn("rank", rank().over(window_spec)) \
                .withColumn("row_num", row_number().over(window_spec)) \
                .withColumn("prev_salary", lag("salary", 1).over(window_spec))
```

### Real-world Transformation Pipeline Example

```python
from pyspark.sql.functions import *

# Complex transformation pipeline
result = df.filter(col("age") > 18) \
           .withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name"))) \
           .withColumn("salary_category",
               when(col("salary") < 50000, "Low")
               .when(col("salary") < 100000, "Medium")
               .otherwise("High")
           ) \
           .groupBy("department", "salary_category") \
           .agg(
               count("*").alias("employee_count"),
               avg("salary").alias("avg_salary"),
               max("salary").alias("max_salary")
           ) \
           .orderBy(col("department"), col("avg_salary").desc())
```

---

## 13. What Spark optimization techniques do you use in real-time projects?

### 1. Caching and Persistence

```python
# Cache frequently accessed DataFrames
df.cache()  # or df.persist()

# Choose appropriate storage level
from pyspark import StorageLevel

df.persist(StorageLevel.MEMORY_AND_DISK)  # Spill to disk if needed
df.persist(StorageLevel.MEMORY_ONLY_SER)  # Serialized (saves memory)

# Unpersist when no longer needed
df.unpersist()
```

**When to cache:**
- DataFrame used multiple times
- Iterative algorithms
- Interactive analysis

### 2. Broadcast Joins

```python
from pyspark.sql.functions import broadcast

# Broadcast small table (<10MB typically)
result = large_df.join(broadcast(small_df), "key")

# Avoid shuffle of large table
# Small table sent to all executors
```

**Benefits:**
- No shuffle for large table
- Faster join execution
- Reduces network I/O

### 3. Partitioning Optimization

```python
# Check current partitions
df.rdd.getNumPartitions()

# Repartition (triggers shuffle)
df_repart = df.repartition(100)

# Coalesce (reduces partitions without shuffle)
df_coal = df.coalesce(10)

# Optimal partition size: 128MB - 1GB per partition
# Optimal partition count: 2-4x number of cores

# Repartition before expensive operations
df_repart = df.repartition(200).groupBy("key").agg(sum("value"))
```

### 4. Predicate Pushdown

```python
# Good: Filter early (pushdown to data source)
df = spark.read.parquet("data.parquet") \
     .filter(col("date") == "2024-01-01")  # Filters at read

# Bad: Filter late
df = spark.read.parquet("data.parquet")
df = df.groupBy("city").count()
df = df.filter(col("date") == "2024-01-01")  # Reads everything first
```

### 5. Column Pruning

```python
# Good: Select only needed columns early
df = spark.read.parquet("data.parquet") \
     .select("id", "name", "age")

# Bad: Select all then filter columns later
df = spark.read.parquet("data.parquet")
# ... many transformations ...
df = df.select("id", "name", "age")
```

### 6. Avoid Shuffles

```python
# Use reduceByKey instead of groupByKey
# Good: Combines locally before shuffle
rdd.reduceByKey(lambda x, y: x + y)

# Bad: Shuffles all data then combines
rdd.groupByKey().mapValues(sum)

# Use aggregations efficiently
df.groupBy("dept").agg(sum("salary"))  # ✅
df.groupBy("dept").apply(lambda x: x.sum())  # ❌ (slower)
```

### 7. Optimize Shuffle Partitions

```python
# Adjust shuffle partitions based on data size
spark.conf.set("spark.sql.shuffle.partitions", "200")  # Default

# For small data
spark.conf.set("spark.sql.shuffle.partitions", "20")

# For large data (TBs)
spark.conf.set("spark.sql.shuffle.partitions", "500")

# Rule of thumb: Target 128MB - 1GB per partition after shuffle
```

### 8. Use Efficient File Formats

```python
# Parquet: Columnar, compressed, supports predicate pushdown
df.write.parquet("output.parquet")

# ORC: Similar to Parquet, better for Hive
df.write.orc("output.orc")

# Avoid CSV/JSON for large datasets
# They don't support:
# - Column pruning
# - Predicate pushdown
# - Efficient compression
```

### 9. Adaptive Query Execution (AQE)

```python
# Enable AQE (Spark 3.0+)
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Benefits:
# - Automatically coalesces shuffle partitions
# - Converts sort-merge join to broadcast join dynamically
# - Handles data skew automatically

# Additional AQE configs
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

### 10. Handle Data Skew

```python
# Problem: One partition has significantly more data
# Symptoms: One task takes much longer

# Solution 1: Salting
df_salted = df.withColumn("salt", (rand() * 10).cast("int"))
result = df_salted.join(other_df, ["key", "salt"])

# Solution 2: Broadcast skewed keys
skewed_keys = df.groupBy("key").count().filter(col("count") > threshold)
# Broadcast skewed data separately

# Solution 3: Use AQE skew join optimization
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

### 11. Bucketing

```python
# Pre-partition data by key
df.write.bucketBy(100, "user_id").saveAsTable("users_bucketed")

# Benefits:
# - Avoids shuffle when joining on bucketed column
# - Faster aggregations on bucketed column

# Join bucketed tables (no shuffle)
df1 = spark.table("users_bucketed")
df2 = spark.table("orders_bucketed")
result = df1.join(df2, "user_id")  # No shuffle!
```

### 12. Partition Writing

```python
# Partition by frequently filtered columns
df.write.partitionBy("year", "month", "day").parquet("output")

# Benefits:
# - Predicate pushdown eliminates reading unnecessary partitions
# - Faster queries with date filters

# Read specific partition
df = spark.read.parquet("output/year=2024/month=01")
```

### 13. Avoid UDFs When Possible

```python
# Bad: Python UDF (slow, requires serialization)
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

@udf(returnType=IntegerType())
def multiply_by_two(x):
    return x * 2

df = df.withColumn("doubled", multiply_by_two(col("value")))

# Good: Use built-in functions
df = df.withColumn("doubled", col("value") * 2)

# If UDF needed: Use Pandas UDF (vectorized)
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf(IntegerType())
def multiply_by_two_pandas(x: pd.Series) -> pd.Series:
    return x * 2
```

### 14. Configure Executor Resources

```bash
# Optimize executor configuration
spark-submit \
  --executor-memory 32G \           # 32-40GB max per executor
  --executor-cores 5 \              # 5 cores optimal
  --num-executors 20 \              # Based on cluster size
  --driver-memory 8G \
  --conf spark.memory.fraction=0.7 \
  --conf spark.memory.storageFraction=0.3
```

### 15. Optimize Joins

```python
# 1. Broadcast join for small tables (<10MB)
result = large_df.join(broadcast(small_df), "key")

# 2. Filter before join
df1_filtered = df1.filter(col("active") == True)
result = df1_filtered.join(df2, "key")

# 3. Select only needed columns before join
df1_select = df1.select("key", "col1", "col2")
df2_select = df2.select("key", "col3")
result = df1_select.join(df2_select, "key")

# 4. Use proper join type
# inner, left, right, full, left_semi, left_anti
```

### 16. Memory Management

```python
# Configure memory fractions
spark.conf.set("spark.memory.fraction", "0.7")          # 70% for Spark
spark.conf.set("spark.memory.storageFraction", "0.3")   # 30% of Spark memory for storage

# Enable off-heap memory
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "10g")
```

### 17. Monitor and Debug

```python
# Use explain() to understand execution plan
df.explain()          # Physical plan
df.explain(True)      # Logical and physical plans

# Check for shuffles and broadcasts in plan
df.explain("formatted")

# Monitor Spark UI:
# - Check shuffle read/write
# - Identify slow stages
# - Look for data skew
# - Monitor memory usage
```

### Real-world Optimization Example

```python
# Unoptimized version
df = spark.read.csv("large_data.csv")
result = df.groupBy("user_id").agg(sum("amount"))
result = result.filter(col("sum(amount)") > 1000)
result.write.csv("output.csv")

# Optimized version
df = spark.read.parquet("large_data.parquet")  # ✅ Parquet format

# ✅ Broadcast small dimension table if needed
# ✅ Filter early
# ✅ Select only needed columns
df_filtered = df.select("user_id", "amount") \
                .filter(col("amount") > 0)

# ✅ Optimize shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "100")

# ✅ Cache if reused
df_filtered.cache()

result = df_filtered.groupBy("user_id") \
                    .agg(sum("amount").alias("total_amount")) \
                    .filter(col("total_amount") > 1000)

# ✅ Use efficient format and partitioning
result.write.partitionBy("user_id") \
      .parquet("output.parquet")

# ✅ Unpersist when done
df_filtered.unpersist()
```

---

## 14. What is repartition and coalesce in Spark and when would you use each?

### Repartition

**Definition:** Increases or decreases the number of partitions with a **full shuffle**.

```python
# Syntax
df_repartitioned = df.repartition(num_partitions)
df_repartitioned = df.repartition("column_name")  # Hash partitioning
df_repartitioned = df.repartition(num_partitions, "column_name")
```

**Characteristics:**
- Triggers full shuffle
- Can increase or decrease partitions
- Even data distribution
- More expensive operation

**Example:**
```python
# Increase partitions from 10 to 100
df_repart = df.repartition(100)

# Repartition by column (useful for joins)
df_repart = df.repartition("user_id")

# Repartition with specific count and column
df_repart = df.repartition(50, "department")
```

### Coalesce

**Definition:** Decreases the number of partitions **without a full shuffle** (when reducing).

```python
# Syntax
df_coalesced = df.coalesce(num_partitions)
```

**Characteristics:**
- Only reduces partitions (cannot increase)
- Minimal or no shuffle
- May result in uneven data distribution
- More efficient than repartition

**Example:**
```python
# Reduce partitions from 100 to 10
df_coal = df.coalesce(10)

# Common use case: Before writing to reduce output files
df.coalesce(1).write.csv("output.csv")  # Single output file
```

### Comparison Table

| Aspect | Repartition | Coalesce |
|--------|-------------|----------|
| **Shuffle** | Always triggers full shuffle | Minimal/no shuffle when reducing |
| **Direction** | Increase or decrease | Only decrease |
| **Distribution** | Even distribution | May be uneven |
| **Performance** | Slower (due to shuffle) | Faster |
| **Use Case** | Increase parallelism, even distribution | Reduce output files, optimize writing |

### When to Use Repartition

**1. Increase Partitions for Better Parallelism**
```python
# Small number of partitions limits parallelism
df = spark.read.csv("data.csv")  # 5 partitions
print(df.rdd.getNumPartitions())  # 5

# Repartition to utilize all cores
# Cluster has 100 cores, so use 200 partitions
df_repart = df.repartition(200)
```

**2. Even Data Distribution**
```python
# After filtering, partitions may be uneven
df_filtered = df.filter(col("amount") > 1000)

# Repartition for even distribution
df_even = df_filtered.repartition(100)
```

**3. Optimize Joins**
```python
# Repartition both DataFrames by join key
df1_repart = df1.repartition("user_id")
df2_repart = df2.repartition("user_id")

# Join is faster (data already partitioned by key)
result = df1_repart.join(df2_repart, "user_id")
```

**4. Prepare for Aggregations**
```python
# Repartition by aggregation key
df_repart = df.repartition("department")

# Aggregation is more efficient
result = df_repart.groupBy("department").agg(sum("salary"))
```

### When to Use Coalesce

**1. Reduce Output Files**
```python
# Avoid creating too many small files
df.write.parquet("output")  # Creates 200 files if 200 partitions

# Reduce to fewer output files
df.coalesce(10).write.parquet("output")  # Creates 10 files
```

**2. Optimize Writing to Storage**
```python
# Single output file (use with caution for large data)
df.coalesce(1).write.csv("single_output.csv")
```

**3. After Filtering (Fewer Records)**
```python
# Start with 200 partitions
df = spark.read.parquet("data.parquet")  # 200 partitions

# Filter reduces data significantly
df_filtered = df.filter(col("country") == "USA")  # Only 5% of data

# Coalesce to reduce overhead
df_optimized = df_filtered.coalesce(20)
```

**4. Before Caching**
```python
# Reduce partitions before caching to save memory
df_filtered = df.filter(col("active") == True)
df_coalesced = df_filtered.coalesce(50)
df_coalesced.cache()
```

### Real-world Examples

#### Example 1: Data Pipeline
```python
# Read large data (automatically partitioned)
df = spark.read.parquet("large_data.parquet")  # 500 partitions

# Heavy transformations benefit from more partitions
df_transformed = df.repartition(1000) \
                   .withColumn("processed", process_udf(col("raw_data"))) \
                   .groupBy("category").agg(sum("value"))

# Reduce partitions before writing
df_transformed.coalesce(50) \
              .write.parquet("output.parquet")
```

#### Example 2: Join Optimization
```python
# Large table
df_large = spark.read.parquet("large_table")  # 1000 partitions

# Small table after filtering
df_small = spark.read.parquet("small_table")
df_small_filtered = df_small.filter(col("active") == True)  # 1000 → 50 partitions

# Coalesce small table before broadcast join
df_small_coalesced = df_small_filtered.coalesce(1)

# Broadcast join
result = df_large.join(broadcast(df_small_coalesced), "key")
```

#### Example 3: Writing to Different Sinks
```python
# Writing to HDFS (many files OK)
df.write.parquet("hdfs://path")  # 200 files

# Writing to local filesystem (fewer files better)
df.coalesce(10).write.csv("file:///local/path")  # 10 files

# Writing single summary file
summary = df.groupBy("category").sum("amount")
summary.coalesce(1).write.csv("summary.csv")  # 1 file
```

### Common Pitfalls

#### Pitfall 1: Over-coalescing
```python
# Bad: Coalesce to 1 partition for large data
df.coalesce(1).write.parquet("output")  # Single executor processes all data!

# Good: Coalesce to reasonable number
df.coalesce(20).write.parquet("output")
```

#### Pitfall 2: Unnecessary Repartitioning
```python
# Bad: Repartitioning before simple operations
df_repart = df.repartition(100)
result = df_repart.filter(col("age") > 25)  # Unnecessary shuffle

# Good: Only repartition when needed
result = df.filter(col("age") > 25)
```

#### Pitfall 3: Wrong Choice
```python
# Bad: Coalesce to increase partitions (doesn't work)
df_coal = df.coalesce(200)  # If df has 100 partitions, still 100

# Good: Use repartition to increase
df_repart = df.repartition(200)  # Now has 200 partitions
```

### Best Practices

**1. Calculate Optimal Partition Count**
```python
# Target: 128MB - 1GB per partition
data_size_gb = 100
target_partition_size_gb = 0.5
optimal_partitions = data_size_gb / target_partition_size_gb  # 200

df_repart = df.repartition(200)
```

**2. Check Partition Count**
```python
# Always check current partitions
print(f"Current partitions: {df.rdd.getNumPartitions()}")
```

**3. Monitor in Spark UI**
- Check shuffle read/write in Stages tab
- Look for uneven task durations (data skew)

**4. Use Adaptively**
```python
# Enable Adaptive Query Execution (Spark 3.0+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# AQE automatically coalesces partitions after shuffle
```

---

## 15. What types of joins are available in Spark and which join is most efficient in terms of performance?

### Types of Joins in Spark

#### **1. Inner Join**
Returns only matching rows from both tables.

```python
# Inner join (default)
result = df1.join(df2, on="key", how="inner")
# OR
result = df1.join(df2, df1.key == df2.key, "inner")
```

**Use case:** When you only want records that exist in both tables.

#### **2. Left Outer Join (Left Join)**
Returns all rows from left table, matching rows from right.

```python
result = df1.join(df2, on="key", how="left")
# OR
result = df1.join(df2, on="key", how="left_outer")
```

**Use case:** Keep all records from primary table, add info from secondary table if available.

#### **3. Right Outer Join (Right Join)**
Returns all rows from right table, matching rows from left.

```python
result = df1.join(df2, on="key", how="right")
# OR
result = df1.join(df2, on="key", how="right_outer")
```

**Use case:** Less common, usually restructure as left join.

#### **4. Full Outer Join (Full Join)**
Returns all rows from both tables.

```python
result = df1.join(df2, on="key", how="full")
# OR
result = df1.join(df2, on="key", how="full_outer")
```

**Use case:** When you need all records from both tables regardless of matches.

#### **5. Left Semi Join**
Returns rows from left table that have matches in right table (like EXISTS in SQL).

```python
result = df1.join(df2, on="key", how="left_semi")
```

**Characteristics:**
- Only returns columns from left table
- No duplicates from right table
- More efficient than inner join when you only need left table columns

**Use case:** Filter left table based on existence in right table.

```python
# Example: Find customers who made purchases
customers = df1.join(purchases, df1.customer_id == purchases.customer_id, "left_semi")
# Result: Only customer columns, no purchase data
```

#### **6. Left Anti Join**
Returns rows from left table that have NO matches in right table (like NOT EXISTS in SQL).

```python
result = df1.join(df2, on="key", how="left_anti")
```

**Use case:** Find records in left table missing from right table.

```python
# Example: Find customers with no purchases
non_buyers = customers.join(purchases, on="customer_id", how="left_anti")
```

#### **7. Cross Join (Cartesian Product)**
Returns Cartesian product of both tables.

```python
result = df1.crossJoin(df2)
# OR
result = df1.join(df2, how="cross")
```

**Warning:** Creates rows = (df1_rows × df2_rows). Very expensive!

**Use case:** Rare - only for small tables or specific analytical needs.

### Join Strategies (Performance)

#### **1. Broadcast Hash Join (Most Efficient)**

**How it works:**
- Small table broadcast to all executors
- Hash join performed locally on each executor
- No shuffle required

```python
from pyspark.sql.functions import broadcast

# Explicitly broadcast small table
result = large_df.join(broadcast(small_df), "key")
```

**Characteristics:**
- ✅ Fastest join strategy
- ✅ No shuffle
- ✅ Works for all join types except full outer
- ⚠️ Small table must fit in executor memory

**When used:**
- Spark automatically uses for tables < `spark.sql.autoBroadcastJoinThreshold` (default 10MB)
- Can manually trigger with `broadcast()` function

**Use case:** One table is significantly smaller (<10MB typically)

#### **2. Shuffle Hash Join**

**How it works:**
- Both tables shuffled by join key
- Hash join performed after shuffle
- Builds hash table from smaller relation

**Characteristics:**
- Requires shuffle of both tables
- Faster than sort-merge for unsorted data
- Memory intensive (needs hash table)

**Configuration:**
```python
spark.conf.set("spark.sql.join.preferSortMergeJoin", "false")
```

**Use case:** Medium-sized tables where neither can be broadcast.

#### **3. Sort-Merge Join (Default for Large Joins)**

**How it works:**
- Both tables shuffled by join key
- Both sides sorted
- Merge-sort algorithm performs join

**Characteristics:**
- Both tables must be sorted
- Memory efficient (streaming join)
- Default for large-large joins

**Configuration:**
```python
spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")  # Default
```

**Use case:** Large tables that cannot be broadcast.

#### **4. Cartesian Join (Least Efficient)**

**How it works:**
- No join keys
- Every row from table1 paired with every row from table2

**Characteristics:**
- Extremely expensive
- Should be avoided unless necessary

**Use case:** Specific analytical queries on very small tables.

### Performance Comparison

| Join Strategy | Shuffle Required | Memory Usage | Speed | Best For |
|--------------|------------------|--------------|-------|----------|
| **Broadcast Hash** | No | Low | ⚡ Fastest | Small table (< 10MB) |
| **Shuffle Hash** | Yes | High | 🏃 Fast | Medium tables |
| **Sort-Merge** | Yes | Medium | 🚶 Moderate | Large tables |
| **Cartesian** | No | Very High | 🐌 Slowest | Avoid |

### Join Optimization Strategies

#### **1. Broadcast Small Tables**
```python
# Automatically broadcast if < 10MB
result = large_df.join(small_df, "key")

# Manually broadcast for tables > 10MB (if fits in memory)
result = large_df.join(broadcast(medium_df), "key")

# Increase broadcast threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "20971520")  # 20MB
```

#### **2. Filter Before Join**
```python
# Bad: Join then filter
result = df1.join(df2, "key").filter(col("date") == "2024-01-01")

# Good: Filter then join
df1_filtered = df1.filter(col("date") == "2024-01-01")
df2_filtered = df2.filter(col("date") == "2024-01-01")
result = df1_filtered.join(df2_filtered, "key")
```

#### **3. Select Only Needed Columns**
```python
# Bad: Join with all columns
result = df1.join(df2, "key")

# Good: Select needed columns first
df1_select = df1.select("key", "col1", "col2")
df2_select = df2.select("key", "col3")
result = df1_select.join(df2_select, "key")
```

#### **4. Repartition by Join Key**
```python
# For large-large joins
df1_repart = df1.repartition("key")
df2_repart = df2.repartition("key")
result = df1_repart.join(df2_repart, "key")
```

#### **5. Handle Data Skew**
```python
# If join key has skewed distribution
# Technique: Salting

# Add random salt to both sides
df1_salted = df1.withColumn("salt", (rand() * 10).cast("int"))
df2_exploded = df2.withColumn("salt", explode(array([lit(i) for i in range(10)])))

result = df1_salted.join(df2_exploded, ["key", "salt"])
result = result.drop("salt")
```

#### **6. Use Bucketing**
```python
# Pre-bucket tables
df1.write.bucketBy(100, "key").saveAsTable("table1_bucketed")
df2.write.bucketBy(100, "key").saveAsTable("table2_bucketed")

# Join bucketed tables (no shuffle!)
df1_bucketed = spark.table("table1_bucketed")
df2_bucketed = spark.table("table2_bucketed")
result = df1_bucketed.join(df2_bucketed, "key")
```

### Real-world Join Examples

#### Example 1: Broadcast Join
```python
# Large fact table (100GB)
sales = spark.read.parquet("sales.parquet")

# Small dimension table (5MB)
products = spark.read.parquet("products.parquet")

# Broadcast join (most efficient)
result = sales.join(broadcast(products), "product_id")
```

#### Example 2: Sort-Merge Join
```python
# Two large tables (50GB each)
customers = spark.read.parquet("customers.parquet")
orders = spark.read.parquet("orders.parquet")

# Sort-merge join (default for large tables)
result = customers.join(orders, "customer_id")
```

#### Example 3: Semi Join
```python
# Find active customers (customers who ordered in last 30 days)
active_customers = customers.join(
    recent_orders,
    on="customer_id",
    how="left_semi"
)
# More efficient than inner join when you don't need order details
```

### Best Practices

**1. Always prefer broadcast joins when possible**
```python
# Check table size first
df.count() * average_row_size < 10MB → Use broadcast
```

**2. Choose appropriate join type**
```python
# Need all columns from both? → Inner/Outer join
# Need only left columns + filter by right? → Semi join
# Need left columns except matching right? → Anti join
```

**3. Monitor join performance in Spark UI**
- Check for shuffle read/write
- Look for broadcast size
- Identify data skew in tasks

**4. Use explain() to verify join strategy**
```python
result.explain()
# Look for: BroadcastHashJoin, SortMergeJoin, etc.
```

---

## 16. What is a broadcast join?

### Broadcast Join

**Definition:** A join optimization where a small table is sent (broadcast) to all executor nodes, allowing the join to happen locally without shuffling the large table.

### How It Works

**Traditional Join (Shuffle):**
```
Large Table (100GB)     Small Table (5MB)
        ↓                      ↓
    Shuffle (expensive)    Shuffle
        ↓                      ↓
    Join on Executors
```

**Broadcast Join:**
```
Large Table (100GB)     Small Table (5MB)
        ↓                      ↓
   No Shuffle          Broadcast to ALL Executors
        ↓                      ↓
    Local Join (each executor has small table copy)
```

### Syntax

```python
from pyspark.sql.functions import broadcast

# Method 1: Explicit broadcast
result = large_df.join(broadcast(small_df), "key")

# Method 2: Automatic (if small_df < 10MB)
result = large_df.join(small_df, "key")
```

### When Spark Uses Broadcast Join Automatically

**Condition:**
```python
# Table size < spark.sql.autoBroadcastJoinThreshold
# Default: 10MB (10485760 bytes)

# Check current threshold
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")

# Increase threshold (if you have memory)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "20971520")  # 20MB
```

**Spark automatically broadcasts if:**
1. Table size < threshold
2. Table statistics are available
3. Not a full outer join (broadcast doesn't support it)

### Advantages

**1. No Shuffle of Large Table**
```python
# Without broadcast: Both tables shuffled
result = large_df.join(small_df, "key")  # Expensive

# With broadcast: Only small table sent to all nodes
result = large_df.join(broadcast(small_df), "key")  # Cheap
```

**2. Faster Execution**
- No shuffle I/O for large table
- Local hash join on each partition
- Significant speedup (can be 10-100x faster)

**3. Reduced Network I/O**
- Small table sent once to each executor
- Large table stays in place

### Limitations

**1. Memory Constraints**
```python
# Small table must fit in executor memory
# Rule of thumb: < 10MB is safe
# With more memory: up to 100MB-200MB possible

# Too large broadcast causes OOM
# Error: "Not enough memory to build broadcast table"
```

**2. Not Suitable for:**
- Full outer joins (broadcast not supported)
- Both tables are large
- Frequent table updates (re-broadcast overhead)

**3. Broadcast Timeout**
```python
# If broadcast takes too long
spark.conf.set("spark.sql.broadcastTimeout", "600")  # 10 minutes
```

### When to Use Broadcast Join

**Perfect Scenarios:**

**1. Dimension-Fact Joins**
```python
# Large fact table (sales)
sales = spark.read.parquet("sales.parquet")  # 100GB

# Small dimension table (products)
products = spark.read.parquet("products.parquet")  # 2MB

# Broadcast dimension
result = sales.join(broadcast(products), "product_id")
```

**2. Lookup Tables**
```python
# Large transactions
transactions = spark.read.parquet("transactions")

# Small lookup table (country codes, categories, etc.)
lookup = spark.read.csv("country_codes.csv")

result = transactions.join(broadcast(lookup), "country_code")
```

**3. Filtered Large Tables**
```python
# Start with large table
users = spark.read.parquet("all_users.parquet")  # 50GB

# Filter to small subset
active_users = users.filter(col("active") == True)  # Now 5MB

# Broadcast filtered result
result = orders.join(broadcast(active_users), "user_id")
```

### Avoid Broadcast Join When:

**1. Both Tables Are Large**
```python
# Both > 100MB
table1 = spark.read.parquet("large_table1")  # 10GB
table2 = spark.read.parquet("large_table2")  # 5GB

# Don't broadcast - use sort-merge join
result = table1.join(table2, "key")
```

**2. Full Outer Joins**
```python
# Broadcast not supported
result = df1.join(df2, "key", "full_outer")  # Uses sort-merge
```

**3. Insufficient Memory**
```python
# If executors have limited memory (< 2GB)
# Avoid broadcasting tables > 50MB
```

### Real-world Examples

#### Example 1: Star Schema Join
```python
# Fact table (very large)
fact_sales = spark.read.parquet("fact_sales")  # 500GB

# Dimension tables (small)
dim_product = spark.read.parquet("dim_product")  # 10MB
dim_customer = spark.read.parquet("dim_customer")  # 5MB
dim_time = spark.read.parquet("dim_time")  # 1MB

# Broadcast all dimensions
result = fact_sales \
    .join(broadcast(dim_product), "product_id") \
    .join(broadcast(dim_customer), "customer_id") \
    .join(broadcast(dim_time), "date_id")

# Result: Fast execution, minimal shuffle
```

#### Example 2: Multiple Joins
```python
# Main table
orders = spark.read.parquet("orders")  # 100GB

# Small lookup tables
customers = spark.read.parquet("customers").filter(col("active") == True)  # 8MB
products = spark.read.csv("products.csv")  # 3MB
regions = spark.read.json("regions.json")  # 500KB

# Chain broadcast joins
result = orders \
    .join(broadcast(customers), "customer_id") \
    .join(broadcast(products), "product_id") \
    .join(broadcast(regions), "region_id") \
    .select("order_id", "customer_name", "product_name", "region_name")
```

#### Example 3: Avoid Over-broadcasting
```python
# Bad: Broadcasting too large table
large_table = spark.read.parquet("large.parquet")  # 500MB
result = df1.join(broadcast(large_table), "key")  # OOM error!

# Good: Use sort-merge join
result = df1.join(large_table, "key")

# Or: Increase executor memory and threshold
spark.conf.set("spark.executor.memory", "10g")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "524288000")  # 500MB
```

### How to Verify Broadcast Join

**Method 1: Use explain()**
```python
result = large_df.join(broadcast(small_df), "key")
result.explain()

# Output should show:
# == Physical Plan ==
# BroadcastHashJoin [key#1], [key#2], Inner
```

**Method 2: Check Spark UI**
- Go to SQL tab
- Look for join stage
- Check for "BroadcastExchange" in DAG

**Method 3: Look for shuffle metrics**
```python
# With broadcast: No shuffle read/write for large table
# Without broadcast: High shuffle read/write for both tables
```

### Optimization Tips

**1. Disable Auto-broadcast if needed**
```python
# If you want to control manually
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # Disable

# Then explicitly broadcast
result = df1.join(broadcast(df2), "key")
```

**2. Cache small table before broadcast**
```python
small_df.cache()
small_df.count()  # Materialize cache

# Now broadcast
result = large_df.join(broadcast(small_df), "key")
```

**3. Monitor broadcast size**
```python
# Check Spark UI → SQL tab → Broadcast size
# Should be < 10MB for safety
```

**4. Consider data skew**
```python
# Even with broadcast, skewed join keys can cause issues
# One executor gets most work

# Solution: Filter or aggregate before broadcast
small_df_agg = small_df.groupBy("key").agg(first("value"))
result = large_df.join(broadcast(small_df_agg), "key")
```

### Common Errors and Solutions

#### Error 1: OOM during broadcast
```
Error: Not enough memory to build broadcasted relation
```

**Solution:**
```python
# Option 1: Increase executor memory
--executor-memory 16G

# Option 2: Reduce broadcast threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "5242880")  # 5MB

# Option 3: Don't broadcast, use regular join
result = df1.join(df2, "key")  # Sort-merge join
```

#### Error 2: Broadcast timeout
```
Error: Could not execute broadcast in 300 seconds
```

**Solution:**
```python
# Increase timeout
spark.conf.set("spark.sql.broadcastTimeout", "900")  # 15 minutes

# Or check if table is too large to broadcast
```

---

*End of Apache Spark / PySpark Guide*

---

## Summary

This guide covered 16 comprehensive questions on Apache Spark/PySpark:
1. DataFrame vs Dataset
2. DAG in Spark
3. Lineage graph
4. Transformations (narrow vs wide)
5. Spark job submission flow
6. Spark architecture
7. Jobs, stages, tasks calculation
8. Partitions in Spark
9. Data locality
10. Client vs cluster mode
11. Memory management
12. Common transformations
13. Optimization techniques
14. Repartition vs coalesce
15. Join types and performance
16. Broadcast joins

Each question includes detailed explanations, code examples, real-world scenarios, and best practices.
