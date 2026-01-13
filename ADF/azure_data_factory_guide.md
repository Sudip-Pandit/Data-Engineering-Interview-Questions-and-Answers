# Azure Data Factory (ADF) Interview Guide

## 55. What is Azure Data Factory?

### Definition

**Azure Data Factory (ADF)** is a cloud-based data integration service that allows you to create data-driven workflows (pipelines) for orchestrating and automating data movement and data transformation at scale.

### Key Concepts

#### **What is ADF?**
- ETL/ELT orchestration service
- Code-free visual interface
- Supports 90+ data connectors
- Serverless (no infrastructure management)
- Pay-per-use pricing model

#### **Core Components**

```
Pipeline (Workflow)
├── Activities (Tasks)
│   ├── Copy Data Activity
│   ├── Data Flow Activity
│   ├── Databricks Notebook Activity
│   ├── Stored Procedure Activity
│   └── Web Activity
├── Datasets (Data definitions)
│   ├── Source Dataset
│   └── Sink Dataset
├── Linked Services (Connections)
│   ├── Azure SQL Database
│   ├── Blob Storage
│   ├── Databricks
│   └── On-premises SQL Server
└── Triggers (Schedulers)
    ├── Schedule Trigger
    ├── Tumbling Window Trigger
    └── Event-based Trigger
```

### Architecture

```
┌─────────────────────────────────────────────────────┐
│              Azure Data Factory                     │
│  ┌──────────────┐         ┌──────────────┐        │
│  │  Pipelines   │────────▶│  Activities  │        │
│  └──────────────┘         └──────────────┘        │
│         │                         │                 │
│         ▼                         ▼                 │
│  ┌──────────────┐         ┌──────────────┐        │
│  │   Datasets   │◀────────│    Linked    │        │
│  │              │         │   Services   │        │
│  └──────────────┘         └──────────────┘        │
└─────────────────────────────────────────────────────┘
         │                         │
         ▼                         ▼
┌─────────────────┐       ┌─────────────────┐
│  Source Systems │       │  Sink Systems   │
│  - SQL DB       │       │  - Data Lake    │
│  - Blob Storage │       │  - SQL DW       │
│  - REST APIs    │       │  - Cosmos DB    │
└─────────────────┘       └─────────────────┘
```

### Core Components Explained

#### **1. Linked Services (Connections)**
Define connection information to data sources.

```json
{
    "name": "AzureSqlDatabase1",
    "type": "AzureSqlDatabase",
    "typeProperties": {
        "connectionString": "Server=tcp:myserver.database.windows.net,1433;Database=mydb;",
        "authenticationType": "SQL",
        "userName": "admin",
        "password": {
            "type": "AzureKeyVaultSecret",
            "store": {
                "referenceName": "AzureKeyVault1",
                "type": "LinkedServiceReference"
            },
            "secretName": "SqlPassword"
        }
    }
}
```

**Types:**
- Data stores (SQL, Blob, ADLS, Cosmos DB)
- Compute services (Databricks, HDInsight, Azure ML)
- Generic (HTTP, REST APIs)

#### **2. Datasets**
Represent data structures within data stores.

```json
{
    "name": "SourceSqlTable",
    "type": "AzureSqlTable",
    "linkedServiceName": {
        "referenceName": "AzureSqlDatabase1",
        "type": "LinkedServiceReference"
    },
    "typeProperties": {
        "schema": "dbo",
        "table": "Customers"
    }
}
```

**Dataset Types:**
- DelimitedText (CSV)
- Parquet
- JSON
- Binary
- Avro
- ORC

#### **3. Pipelines**
Logical grouping of activities that perform a task.

```json
{
    "name": "CopyPipeline",
    "properties": {
        "activities": [
            {
                "name": "CopyFromSqlToBlob",
                "type": "Copy",
                "inputs": [{"referenceName": "SourceSqlTable"}],
                "outputs": [{"referenceName": "SinkBlobDataset"}]
            }
        ]
    }
}
```

#### **4. Activities**
Processing steps within a pipeline.

**Data Movement:**
- Copy Activity: Move data between sources

**Data Transformation:**
- Data Flow: Visual data transformation
- Databricks Notebook: Run Databricks code
- HDInsight: Run Hive/Pig/MapReduce

**Control Flow:**
- If Condition: Conditional branching
- ForEach: Loop over items
- Until: Loop until condition met
- Wait: Delay execution
- Web: Call REST APIs

#### **5. Triggers**
Define when pipelines should execute.

**Types:**
```json
// Schedule Trigger (runs at specific times)
{
    "name": "DailyTrigger",
    "type": "ScheduleTrigger",
    "properties": {
        "recurrence": {
            "frequency": "Day",
            "interval": 1,
            "startTime": "2024-01-01T00:00:00Z",
            "timeZone": "UTC",
            "schedule": {
                "hours": [2],
                "minutes": [0]
            }
        }
    }
}

// Tumbling Window Trigger (processes data in windows)
{
    "name": "HourlyWindow",
    "type": "TumblingWindowTrigger",
    "properties": {
        "frequency": "Hour",
        "interval": 1,
        "startTime": "2024-01-01T00:00:00Z",
        "delay": "00:00:00",
        "retryPolicy": {
            "count": 3,
            "intervalInSeconds": 30
        }
    }
}

// Event-based Trigger (on blob creation)
{
    "name": "BlobEventTrigger",
    "type": "BlobEventsTrigger",
    "properties": {
        "events": ["Microsoft.Storage.BlobCreated"],
        "scope": "/subscriptions/.../blobServices/default/containers/input"
    }
}
```

### Common Use Cases

#### **1. ETL/ELT Pipelines**
```
Extract data from multiple sources
    ↓
Transform using Data Flow or Databricks
    ↓
Load to Data Warehouse or Data Lake
```

#### **2. Data Migration**
```
On-premises SQL Server
    ↓ (Self-hosted Integration Runtime)
Azure SQL Database
    ↓
Azure Synapse Analytics
```

#### **3. Incremental Data Loading**
```
Source: Sales Database (only new/updated records)
    ↓
Staging: Azure Data Lake
    ↓
Target: Azure Synapse Analytics
```

#### **4. Orchestration**
```
Pipeline 1: Ingest data
    ↓
Pipeline 2: Transform with Databricks
    ↓
Pipeline 3: Load to warehouse
    ↓
Pipeline 4: Trigger Power BI refresh
```

### Key Features

**1. Integration Runtime**
- Azure IR: Cloud-to-cloud data movement
- Self-hosted IR: On-premises to cloud
- Azure-SSIS IR: Run SSIS packages

**2. Data Flows**
- Visual data transformation
- Code-free ETL
- Spark-based execution

**3. Monitoring**
- Pipeline run history
- Activity-level monitoring
- Alerts and metrics
- Integration with Azure Monitor

**4. Security**
- Managed Identity
- Azure Key Vault integration
- Private endpoints
- Customer-managed keys

### Simple Pipeline Example

**Scenario:** Copy data from Azure SQL to Blob Storage daily

```json
// 1. Linked Service: Azure SQL
{
    "name": "AzureSqlDB",
    "type": "AzureSqlDatabase",
    "connectionString": "Server=myserver.database.windows.net;Database=mydb;"
}

// 2. Linked Service: Blob Storage
{
    "name": "BlobStorage",
    "type": "AzureBlobStorage",
    "connectionString": "DefaultEndpointsProtocol=https;AccountName=myaccount;"
}

// 3. Dataset: Source SQL Table
{
    "name": "SqlSource",
    "type": "AzureSqlTable",
    "linkedServiceName": "AzureSqlDB",
    "tableName": "dbo.Orders"
}

// 4. Dataset: Sink Blob
{
    "name": "BlobSink",
    "type": "DelimitedText",
    "linkedServiceName": "BlobStorage",
    "location": {
        "container": "output",
        "fileName": "orders.csv"
    }
}

// 5. Pipeline: Copy Activity
{
    "name": "CopyPipeline",
    "activities": [
        {
            "name": "CopyOrdersActivity",
            "type": "Copy",
            "inputs": [{"referenceName": "SqlSource"}],
            "outputs": [{"referenceName": "BlobSink"}],
            "typeProperties": {
                "source": {"type": "SqlSource"},
                "sink": {"type": "DelimitedTextSink"}
            }
        }
    ]
}

// 6. Trigger: Daily at 2 AM
{
    "name": "DailyTrigger",
    "type": "ScheduleTrigger",
    "properties": {
        "recurrence": {
            "frequency": "Day",
            "interval": 1,
            "schedule": {"hours": [2]}
        }
    }
}
```

---

## 56. What is Integration Runtime in ADF and what are the different types?

### Integration Runtime (IR)

**Definition:** Integration Runtime is the compute infrastructure used by Azure Data Factory to provide data integration capabilities across different network environments.

### Types of Integration Runtime

#### **1. Azure Integration Runtime (Azure IR)**

**Purpose:** Cloud-to-cloud data movement and transformation

**Characteristics:**
- Fully managed by Microsoft
- Serverless
- Auto-scaling
- Pay-per-use
- Multiple regions available

**Use Cases:**
```
✓ Copy data between Azure services
✓ Run Data Flows
✓ Execute Databricks notebooks
✓ Call REST APIs
✓ Lookup activities

✗ Access on-premises data (unless via public endpoint)
```

**Configuration:**
```json
{
    "name": "AutoResolveIntegrationRuntime",
    "type": "Managed",
    "properties": {
        "computeProperties": {
            "location": "Auto Resolve",  // or specific region
            "dataFlowProperties": {
                "computeType": "General",
                "coreCount": 8,
                "timeToLive": 10
            }
        }
    }
}
```

#### **2. Self-hosted Integration Runtime (SHIR)**

**Purpose:** Access on-premises or private network resources

**Characteristics:**
- Installed on your own machine/VM
- Supports on-premises and private networks
- Can access data behind firewalls
- Highly available (multi-node)
- Can be shared across data factories

**Use Cases:**
```
✓ Copy from on-premises SQL Server to cloud
✓ Access private VNet resources
✓ Connect to data behind firewall
✓ Hybrid scenarios
```

**Architecture:**
```
On-premises Network
├── Self-hosted IR (Node 1) ─┐
├── Self-hosted IR (Node 2) ─┤── High Availability
└── Self-hosted IR (Node 3) ─┘
         │
         ↓ (HTTPS outbound only)
    Azure Data Factory
         ↓
    Cloud Storage/Services
```

**Installation Steps:**
```powershell
# 1. Download IR from Azure portal
# 2. Install on on-premises machine
# 3. Register with authentication key

# PowerShell installation
msiexec /i IntegrationRuntime.msi INSTALLTYPE=AzureTemplate AUTH_KEY=<your-key>

# Verify installation
Get-Service -Name "IntegrationRuntime*"
```

**Configuration:**
```json
{
    "name": "OnPremisesSHIR",
    "type": "SelfHosted",
    "properties": {
        "description": "On-premises SQL Server access",
        "linkedInfo": {
            "resourceId": "/subscriptions/.../resourceGroups/.../providers/Microsoft.DataFactory/factories/mydf/integrationRuntimes/OnPremisesSHIR"
        }
    }
}
```

#### **3. Azure-SSIS Integration Runtime**

**Purpose:** Run existing SSIS (SQL Server Integration Services) packages in Azure

**Characteristics:**
- Managed SSIS cluster
- Scale up/down
- Supports SSIS catalog (SSISDB)
- Connect to on-premises via VNet

**Use Cases:**
```
✓ Lift-and-shift SSIS packages to Azure
✓ Run existing SSIS workloads
✓ Gradual migration from on-premises
```

**Configuration:**
```json
{
    "name": "AzureSSISIR",
    "type": "Managed",
    "properties": {
        "computeProperties": {
            "location": "East US",
            "nodeSize": "Standard_D4_v3",
            "numberOfNodes": 2,
            "maxParallelExecutionsPerNode": 4
        },
        "ssisProperties": {
            "catalogInfo": {
                "catalogServerEndpoint": "myserver.database.windows.net",
                "catalogAdminUserName": "admin",
                "catalogAdminPassword": {
                    "type": "SecureString",
                    "value": "password"
                },
                "catalogPricingTier": "S1"
            }
        }
    }
}
```

### Comparison Table

| Feature | Azure IR | Self-hosted IR | Azure-SSIS IR |
|---------|----------|----------------|---------------|
| **Location** | Azure regions | Your infrastructure | Azure regions |
| **Management** | Fully managed | Self-managed | Fully managed |
| **Network** | Public internet | Private networks | Public/VNet |
| **Use Case** | Cloud-to-cloud | Hybrid scenarios | SSIS packages |
| **Scaling** | Automatic | Manual | Scale up/down |
| **Cost** | Pay-per-use | VM costs | Pay-per-hour |
| **Data Flow** | ✓ | ✗ | ✗ |
| **On-premises** | ✗ | ✓ | ✗ |

### Integration Runtime Selection Decision Tree

```
Need to run SSIS packages?
├── Yes → Azure-SSIS IR
└── No
    ├── Need on-premises/private network access?
    │   ├── Yes → Self-hosted IR
    │   └── No → Azure IR
    └── Pure cloud-to-cloud?
        └── Yes → Azure IR
```

### Real-world Examples

#### **Example 1: Hybrid Scenario (Self-hosted IR)**

```
Requirement:
- Copy data from on-premises SQL Server to Azure Data Lake
- On-premises SQL not accessible via public internet
- Firewall restrictions

Solution:
1. Install Self-hosted IR on-premises server
2. Create linked service with Self-hosted IR
3. Copy data to cloud

// Linked Service using SHIR
{
    "name": "OnPremSqlServer",
    "type": "SqlServer",
    "connectVia": {
        "referenceName": "OnPremisesSHIR",  // Use Self-hosted IR
        "type": "IntegrationRuntimeReference"
    },
    "properties": {
        "connectionString": "Server=192.168.1.100;Database=SalesDB;",
        "userName": "admin",
        "password": {"type": "SecureString", "value": "***"}
    }
}
```

#### **Example 2: Cloud-to-Cloud (Azure IR)**

```
Requirement:
- Copy data from Azure SQL to Azure Data Lake
- Both in same region (East US)
- No on-premises connectivity needed

Solution:
- Use auto-resolve Azure IR (default)
- Fast, serverless, cost-effective

// Pipeline with Azure IR
{
    "name": "CloudCopyPipeline",
    "activities": [
        {
            "name": "Copy",
            "type": "Copy",
            "inputs": [{"referenceName": "AzureSqlSource"}],
            "outputs": [{"referenceName": "DataLakeSink"}]
            // Uses Azure IR automatically
        }
    ]
}
```

#### **Example 3: Data Transformation (Azure IR with Data Flow)**

```
Requirement:
- Transform data using visual Data Flow
- Clean, aggregate, join datasets
- Load to data warehouse

Solution:
- Use Azure IR with Data Flow properties
- Spark-based transformation

// Azure IR for Data Flow
{
    "name": "DataFlowIR",
    "type": "Managed",
    "properties": {
        "dataFlowProperties": {
            "computeType": "MemoryOptimized",
            "coreCount": 16,
            "timeToLive": 10  // minutes to keep cluster alive
        }
    }
}

// Data Flow Activity
{
    "name": "TransformData",
    "type": "ExecuteDataFlow",
    "typeProperties": {
        "dataFlow": {"referenceName": "MyDataFlow"},
        "integrationRuntime": {"referenceName": "DataFlowIR"}
    }
}
```

### High Availability for Self-hosted IR

**Multi-node Setup:**
```
Purpose: Prevent single point of failure

Configuration:
1. Install IR on multiple machines (up to 4 nodes)
2. Register all with same authentication key
3. ADF automatically load balances

Benefits:
- Failover if one node fails
- Better performance (parallel processing)
- Scheduled maintenance without downtime

Example:
Node 1 (Primary): 192.168.1.100
Node 2 (Secondary): 192.168.1.101
Node 3 (Secondary): 192.168.1.102

If Node 1 fails → Traffic automatically routes to Node 2 & 3
```

### Best Practices

**1. Azure IR:**
- Use "Auto Resolve" for location unless specific region needed
- Configure Data Flow TTL appropriately (balance cost vs startup time)
- Monitor Data Flow cluster usage

**2. Self-hosted IR:**
- Install on dedicated machine (not application server)
- Use multiple nodes for high availability
- Monitor resource usage (CPU, memory, network)
- Keep IR version updated
- Secure with NSG rules

**3. Azure-SSIS IR:**
- Right-size node count and size
- Stop when not in use to save costs
- Use SSISDB for package management
- Configure VNet for on-premises connectivity

**4. General:**
- Choose IR type based on data location and network
- Use managed identity for authentication when possible
- Monitor IR metrics in Azure portal
- Set up alerts for IR failures

---

## 57. What is the Copy Data activity in ADF and how does batch count impact performance?

### Copy Data Activity

**Definition:** The most commonly used activity in ADF for copying data from a source to a destination (sink).

**Purpose:**
- Move data between 90+ supported connectors
- Transform data during copy (basic transformations)
- Parallel data copying
- Fault tolerance with retry logic

### Basic Copy Activity Structure

```json
{
    "name": "CopyActivity",
    "type": "Copy",
    "inputs": [{"referenceName": "SourceDataset"}],
    "outputs": [{"referenceName": "SinkDataset"}],
    "typeProperties": {
        "source": {
            "type": "SqlSource",
            "sqlReaderQuery": "SELECT * FROM Orders WHERE OrderDate >= '2024-01-01'"
        },
        "sink": {
            "type": "ParquetSink",
            "storeSettings": {
                "type": "AzureBlobStorageWriteSettings"
            }
        },
        "enableStaging": false,
        "parallelCopies": 4,
        "dataIntegrationUnits": 32
    }
}
```

### Key Performance Settings

#### **1. Data Integration Units (DIUs)**

**What are DIUs?**
- Measure of compute power for Copy activity
- 1 DIU = 1 vCore + 4GB memory
- Range: 2-256 DIUs
- Auto-scale by default

**Formula:**
```
DIUs = vCores × Memory Factor

Default: Auto (2-256 DIUs based on source/sink)
Manual: Specify exact number (2, 4, 8, 16, 32, 64, 128, 256)
```

**Configuration:**
```json
{
    "typeProperties": {
        "dataIntegrationUnits": 32,  // Fixed
        // OR
        "dataIntegrationUnits": 0    // Auto (recommended)
    }
}
```

**Impact:**
```
2 DIUs:  ~200 MB/s throughput
4 DIUs:  ~400 MB/s
8 DIUs:  ~800 MB/s
16 DIUs: ~1.6 GB/s
32 DIUs: ~3.2 GB/s (diminishing returns after this)
```

#### **2. Parallel Copies**

**What is Parallel Copies?**
- Number of parallel threads for copy operation
- Default: Calculated based on source/sink
- Range: 1-256

**Configuration:**
```json
{
    "typeProperties": {
        "parallelCopies": 8  // 8 parallel threads
    }
}
```

**When to use:**
```
High parallelCopies:
- Large files
- Many small files
- High bandwidth network
- Partitioned sources (multiple physical partitions)

Low parallelCopies:
- Small dataset
- Single file
- Limited network bandwidth
- To avoid overwhelming source/sink
```

#### **3. Degree of Copy Parallelism**

**Formula:**
```
Actual Parallelism = MIN(
    parallelCopies setting,
    Physical partitions in source,
    Available DIUs
)
```

**Example:**
```
Configuration:
- parallelCopies: 32
- Source: SQL table with 8 partitions
- DIUs: 16

Actual parallelism = MIN(32, 8, 16) = 8 threads
```

### Batch Count (For Source/Sink Systems)

**Definition:** Number of rows to read/write in each batch operation.

**Where it applies:**
- SQL databases (batch inserts)
- Cosmos DB (batch writes)
- REST APIs (batch requests)

**Configuration:**
```json
{
    "sink": {
        "type": "AzureSqlSink",
        "writeBatchSize": 10000,      // Rows per batch
        "writeBatchTimeout": "00:30:00" // Timeout per batch
    }
}
```

### How Batch Count Impacts Performance

#### **Scenario 1: SQL Sink**

**Small Batch Size (1,000 rows):**
```
Total rows: 1,000,000
Batch size: 1,000
Number of batches: 1,000

Impact:
- More network round trips (1,000 trips)
- Higher overhead
- Slower performance
- Time: ~100 minutes
```

**Large Batch Size (100,000 rows):**
```
Total rows: 1,000,000
Batch size: 100,000
Number of batches: 10

Impact:
- Fewer network round trips (10 trips)
- Lower overhead
- Faster performance
- Time: ~10 minutes
```

**Optimal Batch Size (10,000-50,000):**
```
Total rows: 1,000,000
Batch size: 10,000
Number of batches: 100

Impact:
- Balanced approach
- Good throughput
- Manageable memory
- Time: ~15 minutes
```

#### **Trade-offs**

```
Too Small (< 1,000):
✗ High overhead
✗ Many network calls
✗ Slow performance
✓ Lower memory usage
✓ Better for failures (smaller rollback)

Too Large (> 100,000):
✗ High memory usage
✗ Timeout risks
✗ Large rollback on failure
✓ Fewer network calls
✓ Lower overhead

Optimal (10,000-50,000):
✓ Balanced performance
✓ Reasonable memory
✓ Good recovery
✓ Efficient throughput
```

### Real-world Performance Tuning Example

**Scenario:** Copy 500GB (100M rows) from Azure SQL to Data Lake

**Initial Configuration (Slow):**
```json
{
    "parallelCopies": 1,
    "dataIntegrationUnits": 4,
    "source": {
        "type": "SqlSource"
    },
    "sink": {
        "type": "ParquetSink",
        "writeBatchSize": 1000
    }
}
// Result: 8 hours
```

**Optimized Configuration (Fast):**
```json
{
    "parallelCopies": 16,
    "dataIntegrationUnits": 32,
    "source": {
        "type": "SqlSource",
        "partitionOption": "DynamicRange",  // Parallel reads
        "partitionSettings": {
            "partitionColumnName": "OrderID",
            "partitionUpperBound": "100000000",
            "partitionLowerBound": "1"
        }
    },
    "sink": {
        "type": "ParquetSink",
        "writeBatchSize": 50000,  // Optimized batch size
        "writeBatchTimeout": "00:30:00"
    },
    "enableStaging": true,  // Use staging for better performance
    "stagingSettings": {
        "linkedServiceName": {"referenceName": "BlobStaging"},
        "path": "staging"
    }
}
// Result: 1.5 hours
```

### Performance Monitoring

**Metrics to Check:**
```
1. Copy Duration: Total time taken
2. Throughput: MB/s or rows/s
3. Data Read: Total data read from source
4. Data Written: Total data written to sink
5. Rows Read: Number of rows processed
6. DIUs Used: Average DIUs consumed
7. Parallel Copies Used: Actual parallelism
```

**In Azure Portal:**
```
Pipeline Run → Copy Activity → Details

Example Output:
Duration: 00:45:32
Throughput: 125.6 MB/s
Data read: 324.8 GB
Data written: 324.8 GB
Rows read: 85,234,567
DIUs used: 28 (avg)
Parallel copies: 16
```

### Best Practices

**1. Start with Auto Settings:**
```json
{
    "dataIntegrationUnits": 0,  // Auto
    "parallelCopies": 0         // Auto
}
// Let ADF optimize, then tune if needed
```

**2. Monitor and Iterate:**
```
1. Run with default settings
2. Check performance metrics
3. Identify bottleneck (source, sink, network)
4. Adjust settings
5. Re-test
```

**3. Batch Size Guidelines:**
```
SQL databases: 10,000-50,000 rows
Cosmos DB: 100-1,000 documents
File formats: Not applicable (file-level parallelism)
REST APIs: Depends on API limits
```

**4. Use Partitioning:**
```json
// Enable parallel reads from SQL
{
    "source": {
        "type": "SqlSource",
        "partitionOption": "DynamicRange",
        "partitionSettings": {
            "partitionColumnName": "ID",
            "partitionUpperBound": "1000000",
            "partitionLowerBound": "1"
        }
    }
}
```

**5. Enable Staging for Large Transfers:**
```json
{
    "enableStaging": true,
    "stagingSettings": {
        "linkedServiceName": {"referenceName": "BlobStaging"},
        "path": "staging",
        "enableCompression": true
    }
}
// Use when copying > 100GB between regions
```

### Troubleshooting Slow Copy

**Problem: Slow copy performance**

**Diagnosis:**
```
1. Check Copy Activity metrics
2. Identify bottleneck:
   - Low throughput (<50 MB/s) → Increase DIUs
   - High duration, low rows/s → Increase parallel copies
   - Network latency → Enable staging
   - Source throttling → Reduce parallel copies
```

**Solutions:**
```
Bottleneck: Source Database
→ Add partitioning
→ Optimize source query
→ Index partition column

Bottleneck: Network
→ Enable staging
→ Use compression
→ Copy during off-peak

Bottleneck: Sink
→ Increase batch size
→ Optimize sink (indexes off during load)
→ Use bulk insert mode
```

---

## 58. What is the ForEach activity in ADF and how does batch count work?

### ForEach Activity

**Definition:** A looping control flow activity that iterates over a collection and executes activities for each item.

**Purpose:**
- Process multiple files
- Iterate over tables
- Dynamic pipeline execution
- Parallel processing

### Basic ForEach Structure

```json
{
    "name": "ForEachActivity",
    "type": "ForEach",
    "typeProperties": {
        "items": {
            "value": "@pipeline().parameters.FileList",
            "type": "Expression"
        },
        "isSequential": false,  // Parallel execution
        "batchCount": 20,       // Number of parallel iterations
        "activities": [
            {
                "name": "CopyFile",
                "type": "Copy",
                "inputs": [{"referenceName": "@item()"}],
                "outputs": [{"referenceName": "Destination"}]
            }
        ]
    }
}
```

### Key Properties

#### **1. items (Collection)**
Array of items to iterate over.

```json
// Static array
{
    "items": {
        "value": ["file1.csv", "file2.csv", "file3.csv"],
        "type": "Expression"
    }
}

// Dynamic from parameter
{
    "items": {
        "value": "@pipeline().parameters.FileList",
        "type": "Expression"
    }
}

// From previous activity output
{
    "items": {
        "value": "@activity('GetMetadata').output.childItems",
        "type": "Expression"
    }
}
```

#### **2. isSequential**
Controls execution mode.

```json
// Parallel (default: false)
{
    "isSequential": false  // Process items in parallel
}

// Sequential (true)
{
    "isSequential": true   // Process one at a time
}
```

#### **3. batchCount**
Number of parallel iterations when `isSequential: false`.

```json
{
    "batchCount": 20  // Run 20 iterations in parallel
}
```

**Default:** 20
**Range:** 1-50
**Impact:** See detailed section below

### How Batch Count Works

**Concept:**
```
Total items: 100 files
batchCount: 20
isSequential: false

Execution:
Wave 1: Process files 1-20 (parallel)
Wave 2: Process files 21-40 (parallel)
Wave 3: Process files 41-60 (parallel)
Wave 4: Process files 61-80 (parallel)
Wave 5: Process files 81-100 (parallel)

Total waves: 100 / 20 = 5 waves
```

### Batch Count Impact on Performance

#### **Scenario 1: Small Batch Count**

```json
{
    "batchCount": 5,
    "items": ["file1", "file2", ..., "file100"]  // 100 files
}

Execution:
Wave 1: 5 files (parallel)
Wave 2: 5 files (parallel)
...
Wave 20: 5 files (parallel)

Total waves: 20
Duration per wave: 2 minutes
Total time: 20 × 2 = 40 minutes

Impact:
✗ Slower (more waves)
✓ Lower resource usage
✓ Better for limited resources
```

#### **Scenario 2: Large Batch Count**

```json
{
    "batchCount": 50,  // Maximum
    "items": ["file1", "file2", ..., "file100"]  // 100 files
}

Execution:
Wave 1: 50 files (parallel)
Wave 2: 50 files (parallel)

Total waves: 2
Duration per wave: 2 minutes
Total time: 2 × 2 = 4 minutes

Impact:
✓ Faster (fewer waves)
✗ Higher resource usage
✗ Potential throttling
```

#### **Scenario 3: Optimal Batch Count**

```json
{
    "batchCount": 20,  // Default
    "items": ["file1", "file2", ..., "file100"]  // 100 files
}

Execution:
Wave 1: 20 files (parallel)
Wave 2: 20 files (parallel)
Wave 3: 20 files (parallel)
Wave 4: 20 files (parallel)
Wave 5: 20 files (parallel)

Total waves: 5
Duration per wave: 2 minutes
Total time: 5 × 2 = 10 minutes

Impact:
✓ Balanced performance
✓ Manageable resource usage
✓ Reduced throttling risk
```

### Real-world Examples

#### **Example 1: Process Multiple Files**

**Scenario:** Copy 200 CSV files from Blob to SQL

```json
{
    "name": "ProcessFiles",
    "type": "ForEach",
    "typeProperties": {
        "items": {
            "value": "@activity('GetFileList').output.childItems",
            "type": "Expression"
        },
        "isSequential": false,
        "batchCount": 30,  // 30 files at a time
        "activities": [
            {
                "name": "CopyFileToSQL",
                "type": "Copy",
                "inputs": [
                    {
                        "referenceName": "SourceBlob",
                        "parameters": {
                            "FileName": "@item().name"
                        }
                    }
                ],
                "outputs": [{"referenceName": "SqlTable"}],
                "typeProperties": {
                    "source": {"type": "DelimitedTextSource"},
                    "sink": {"type": "SqlSink"}
                }
            }
        ]
    }
}

// Execution:
// Wave 1: Files 1-30 (parallel)
// Wave 2: Files 31-60 (parallel)
// ...
// Wave 7: Files 181-200 (parallel)
//
// Total time: ~14 minutes (vs 6+ hours sequential)
```

#### **Example 2: Process Multiple Tables**

**Scenario:** Backup 50 SQL tables to Data Lake

```json
{
    "name": "BackupAllTables",
    "type": "ForEach",
    "typeProperties": {
        "items": {
            "value": "@pipeline().parameters.TableList",
            "type": "Expression"
        },
        "isSequential": false,
        "batchCount": 10,  // 10 tables at a time
        "activities": [
            {
                "name": "BackupTable",
                "type": "Copy",
                "inputs": [
                    {
                        "referenceName": "SqlTable",
                        "parameters": {
                            "TableName": "@item().TableName"
                        }
                    }
                ],
                "outputs": [
                    {
                        "referenceName": "DataLake",
                        "parameters": {
                            "FileName": "@concat(item().TableName, '.parquet')"
                        }
                    }
                ],
                "typeProperties": {
                    "source": {"type": "SqlSource"},
                    "sink": {"type": "ParquetSink"}
                }
            }
        ]
    }
}

// With batchCount=10:
// Wave 1: 10 tables
// Wave 2: 10 tables
// ...
// Wave 5: 10 tables
//
// Total time: ~25 minutes
```

#### **Example 3: Calling Databricks Notebooks**

**Scenario:** Run Databricks notebook for each date partition

```json
{
    "name": "ProcessDailyData",
    "type": "ForEach",
    "typeProperties": {
        "items": {
            "value": "@activity('GetDateRange').output.value",
            "type": "Expression"
        },
        "isSequential": false,
        "batchCount": 5,  // Process 5 dates concurrently
        "activities": [
            {
                "name": "RunDatabricksNotebook",
                "type": "DatabricksNotebook",
                "typeProperties": {
                    "notebookPath": "/ETL/ProcessData",
                    "baseParameters": {
                        "date": "@item().date"
                    }
                },
                "linkedServiceName": {
                    "referenceName": "DatabricksLinkedService",
                    "type": "LinkedServiceReference"
                }
            }
        ]
    }
}

// DateRange: 30 days
// batchCount: 5
// Waves: 6 waves of 5 days each
```

### Nested ForEach

**Scenario:** Process files in multiple folders

```json
{
    "name": "OuterForEach_Folders",
    "type": "ForEach",
    "typeProperties": {
        "items": {
            "value": ["folder1", "folder2", "folder3"],
            "type": "Expression"
        },
        "isSequential": false,
        "batchCount": 3,
        "activities": [
            {
                "name": "InnerForEach_Files",
                "type": "ForEach",
                "typeProperties": {
                    "items": {
                        "value": "@activity('GetFiles').output.childItems",
                        "type": "Expression"
                    },
                    "isSequential": false,
                    "batchCount": 20,
                    "activities": [
                        {
                            "name": "ProcessFile",
                            "type": "Copy",
                            "inputs": [{"referenceName": "SourceFile"}],
                            "outputs": [{"referenceName": "DestinationFile"}]
                        }
                    ]
                }
            }
        ]
    }
}

// Parallelism: 3 folders × 20 files = Up to 60 concurrent operations
```

### Choosing the Right Batch Count

**Consider:**

**1. Number of Items**
```
< 20 items: batchCount = number of items
20-100 items: batchCount = 20 (default)
100-500 items: batchCount = 30-40
> 500 items: batchCount = 50 (max)
```

**2. Activity Duration**
```
Quick activities (< 1 min): Higher batchCount (30-50)
Medium activities (1-5 min): Medium batchCount (20-30)
Long activities (> 5 min): Lower batchCount (10-20)
```

**3. Resource Constraints**
```
Source/Sink throttling: Lower batchCount
Abundant resources: Higher batchCount
Shared resources: Lower batchCount
```

**4. Data Size**
```
Small files/tables: Higher batchCount
Large files/tables: Lower batchCount
```

### Monitoring ForEach

**In Azure Portal:**
```
Pipeline Run → ForEach Activity → Input/Output

Output shows:
{
    "count": 100,              // Total items
    "value": [...],            // Items processed
    "effectiveIntegrationRuntime": "AutoResolveIntegrationRuntime",
    "executionDuration": 600,  // Total seconds
    "billingReference": {
        "activityType": "PipelineActivity",
        "billableDuration": 10  // Billed hours
    }
}
```

**Check Individual Iterations:**
```
ForEach Activity → Activity Runs Inside ForEach

Shows:
- Status of each iteration
- Duration per iteration
- Errors (if any)
- Retry information
```

### Best Practices

**1. Start with Default (20)**
```json
{
    "batchCount": 20  // Good starting point
}
```

**2. Monitor and Adjust**
```
1. Run pipeline
2. Check execution time
3. Look for throttling errors
4. Adjust batchCount
5. Re-test
```

**3. Use Sequential for Ordered Processing**
```json
{
    "isSequential": true,  // When order matters
    "batchCount": 1        // Not used when sequential
}
```

**4. Handle Failures Gracefully**
```json
{
    "activities": [
        {
            "name": "ProcessItem",
            "type": "Copy",
            "policy": {
                "retry": 3,
                "retryIntervalInSeconds": 30,
                "secureOutput": false,
                "secureInput": false
            }
        }
    ]
}
```

**5. Use Variables to Track Progress**
```json
// Set variable for success count
{
    "name": "SetSuccessCount",
    "type": "SetVariable",
    "typeProperties": {
        "variableName": "SuccessCount",
        "value": "@add(variables('SuccessCount'), 1)"
    }
}
```

### Common Pitfalls

**1. Too High Batch Count**
```
Problem: Overwhelming source/sink
Solution: Reduce batchCount, monitor throttling

Example:
- batchCount: 50
- Error: "429 Too Many Requests"
- Fix: Reduce to 20
```

**2. Ignoring Activity Duration**
```
Problem: Long-running activities with high batchCount
Result: Many concurrent long operations → resource exhaustion

Solution: Lower batchCount for long activities

Example:
- Activity: Databricks notebook (15 min each)
- batchCount: 50
- Impact: 50 concurrent clusters → cost explosion
- Fix: batchCount: 5
```

**3. Not Handling Partial Failures**
```
Problem: One failure doesn't stop ForEach
Result: Some items processed, some failed

Solution: Check ForEach output, handle failures

{
    "name": "CheckFailures",
    "type": "IfCondition",
    "dependsOn": [{"activity": "ForEach"}],
    "typeProperties": {
        "expression": {
            "value": "@greater(activity('ForEach').output.failedItemsCount, 0)",
            "type": "Expression"
        },
        "ifTrueActivities": [
            {
                "name": "SendAlert",
                "type": "WebActivity"
            }
        ]
    }
}
```

---

## 59. What is the Lookup activity in ADF?

### Lookup Activity

**Definition:** Reads data from a data source and returns the result as a single row or an array of rows.

**Purpose:**
- Read configuration data
- Get file lists
- Retrieve metadata
- Dynamic parameter values
- Control flow decisions

### Basic Lookup Structure

```json
{
    "name": "LookupActivity",
    "type": "Lookup",
    "typeProperties": {
        "source": {
            "type": "AzureSqlSource",
            "sqlReaderQuery": "SELECT * FROM Config WHERE Environment = 'Production'"
        },
        "dataset": {
            "referenceName": "ConfigTable",
            "type": "DatasetReference"
        },
        "firstRowOnly": false  // Return all rows (array) or just first row
    }
}
```

### Key Properties

#### **1. firstRowOnly**

```json
// Return single row (default: true)
{
    "firstRowOnly": true
}
// Output: {"ConfigKey": "value1", "ConfigValue": "value2"}

// Return all rows (array)
{
    "firstRowOnly": false
}
// Output: {
//     "count": 3,
//     "value": [
//         {"ConfigKey": "key1", "ConfigValue": "value1"},
//         {"ConfigKey": "key2", "ConfigValue": "value2"},
//         {"ConfigKey": "key3", "ConfigValue": "value3"}
//     ]
// }
```

### Use Cases

#### **Use Case 1: Get Configuration Values**

```json
// Lookup configuration
{
    "name": "GetConfig",
    "type": "Lookup",
    "typeProperties": {
        "source": {
            "type": "AzureSqlSource",
            "sqlReaderQuery": "SELECT SourcePath, DestPath FROM ETLConfig WHERE Active = 1"
        },
        "dataset": {"referenceName": "ConfigDB"},
        "firstRowOnly": true
    }
}

// Use in Copy activity
{
    "name": "CopyData",
    "type": "Copy",
    "dependsOn": [{"activity": "GetConfig"}],
    "inputs": [
        {
            "referenceName": "SourceDataset",
            "parameters": {
                "path": "@activity('GetConfig').output.firstRow.SourcePath"
            }
        }
    ],
    "outputs": [
        {
            "referenceName": "SinkDataset",
            "parameters": {
                "path": "@activity('GetConfig').output.firstRow.DestPath"
            }
        }
    ]
}
```

#### **Use Case 2: Get File List for ForEach**

```json
// Get list of files
{
    "name": "GetFileList",
    "type": "Lookup",
    "typeProperties": {
        "source": {
            "type": "AzureSqlSource",
            "sqlReaderQuery": "SELECT FileName FROM FilesToProcess WHERE Processed = 0"
        },
        "dataset": {"referenceName": "MetadataDB"},
        "firstRowOnly": false  // Return all files
    }
}

// Iterate over files
{
    "name": "ProcessEachFile",
    "type": "ForEach",
    "dependsOn": [{"activity": "GetFileList"}],
    "typeProperties": {
        "items": {
            "value": "@activity('GetFileList').output.value",
            "type": "Expression"
        },
        "activities": [
            {
                "name": "ProcessFile",
                "type": "Copy",
                "inputs": [
                    {
                        "referenceName": "SourceFile",
                        "parameters": {
                            "fileName": "@item().FileName"
                        }
                    }
                ]
            }
        ]
    }
}
```

#### **Use Case 3: Get Watermark for Incremental Load**

```json
// Get last processed timestamp
{
    "name": "GetWatermark",
    "type": "Lookup",
    "typeProperties": {
        "source": {
            "type": "AzureSqlSource",
            "sqlReaderQuery": "SELECT MAX(LastModifiedDate) as WatermarkValue FROM WatermarkTable WHERE TableName = 'Orders'"
        },
        "dataset": {"referenceName": "WatermarkDB"},
        "firstRowOnly": true
    }
}

// Copy only new/updated records
{
    "name": "IncrementalCopy",
    "type": "Copy",
    "dependsOn": [{"activity": "GetWatermark"}],
    "typeProperties": {
        "source": {
            "type": "SqlSource",
            "sqlReaderQuery": {
                "value": "@concat('SELECT * FROM Orders WHERE ModifiedDate > ''', activity('GetWatermark').output.firstRow.WatermarkValue, '''')",
                "type": "Expression"
            }
        },
        "sink": {"type": "ParquetSink"}
    }
}

// Update watermark
{
    "name": "UpdateWatermark",
    "type": "SqlServerStoredProcedure",
    "dependsOn": [{"activity": "IncrementalCopy"}],
    "typeProperties": {
        "storedProcedureName": "usp_UpdateWatermark",
        "storedProcedureParameters": {
            "TableName": "Orders",
            "WatermarkValue": "@activity('IncrementalCopy').output.executionDetails[0].source.lastModifiedTime"
        }
    }
}
```

#### **Use Case 4: Conditional Execution**

```json
// Check if data exists
{
    "name": "CheckDataExists",
    "type": "Lookup",
    "typeProperties": {
        "source": {
            "type": "AzureSqlSource",
            "sqlReaderQuery": "SELECT COUNT(*) as RecordCount FROM StagingTable"
        },
        "dataset": {"referenceName": "StagingDB"},
        "firstRowOnly": true
    }
}

// Execute only if data exists
{
    "name": "ProcessIfDataExists",
    "type": "IfCondition",
    "dependsOn": [{"activity": "CheckDataExists"}],
    "typeProperties": {
        "expression": {
            "value": "@greater(activity('CheckDataExists').output.firstRow.RecordCount, 0)",
            "type": "Expression"
        },
        "ifTrueActivities": [
            {
                "name": "ProcessData",
                "type": "Copy"
            }
        ],
        "ifFalseActivities": [
            {
                "name": "LogNoData",
                "type": "WebActivity"
            }
        ]
    }
}
```

### Real-world Example: Dynamic Pipeline

**Scenario:** ETL pipeline that processes different tables based on configuration

```json
// Step 1: Get list of tables to process
{
    "name": "GetTableList",
    "type": "Lookup",
    "typeProperties": {
        "source": {
            "type": "AzureSqlSource",
            "sqlReaderQuery": "SELECT TableName, SourceQuery, DestinationPath FROM ETLConfig WHERE Active = 1"
        },
        "dataset": {"referenceName": "ConfigDB"},
        "firstRowOnly": false
    }
}

// Step 2: Process each table
{
    "name": "ProcessEachTable",
    "type": "ForEach",
    "dependsOn": [{"activity": "GetTableList"}],
    "typeProperties": {
        "items": {
            "value": "@activity('GetTableList').output.value",
            "type": "Expression"
        },
        "batchCount": 10,
        "activities": [
            // Step 2a: Get row count
            {
                "name": "GetRowCount",
                "type": "Lookup",
                "typeProperties": {
                    "source": {
                        "type": "AzureSqlSource",
                        "sqlReaderQuery": {
                            "value": "@concat('SELECT COUNT(*) as RowCount FROM ', item().TableName)",
                            "type": "Expression"
                        }
                    },
                    "dataset": {"referenceName": "SourceDB"},
                    "firstRowOnly": true
                }
            },
            // Step 2b: Copy data
            {
                "name": "CopyTableData",
                "type": "Copy",
                "dependsOn": [{"activity": "GetRowCount"}],
                "typeProperties": {
                    "source": {
                        "type": "AzureSqlSource",
                        "sqlReaderQuery": {
                            "value": "@item().SourceQuery",
                            "type": "Expression"
                        }
                    },
                    "sink": {
                        "type": "ParquetSink"
                    }
                },
                "inputs": [{"referenceName": "SourceDB"}],
                "outputs": [
                    {
                        "referenceName": "DataLake",
                        "parameters": {
                            "path": "@item().DestinationPath"
                        }
                    }
                ]
            },
            // Step 2c: Log completion
            {
                "name": "LogCompletion",
                "type": "SqlServerStoredProcedure",
                "dependsOn": [{"activity": "CopyTableData"}],
                "typeProperties": {
                    "storedProcedureName": "usp_LogETL",
                    "storedProcedureParameters": {
                        "TableName": "@item().TableName",
                        "RowCount": "@activity('GetRowCount').output.firstRow.RowCount",
                        "Status": "Success"
                    }
                }
            }
        ]
    }
}
```

### Limitations

**1. Row Limit:**
```
Maximum rows returned: 5,000
If more rows needed: Use Copy activity to intermediate storage
```

**2. Timeout:**
```
Default timeout: 7 days
Can be configured in activity policy
```

**3. Data Size:**
```
Not suitable for large datasets
Use for metadata/configuration only
For large data: Use Copy activity
```

### Best Practices

**1. Always Handle Null Results:**
```json
{
    "name": "SafeLookup",
    "type": "IfCondition",
    "dependsOn": [{"activity": "Lookup"}],
    "typeProperties": {
        "expression": {
            "value": "@and(not(equals(activity('Lookup').output, null)), greater(activity('Lookup').output.count, 0))",
            "type": "Expression"
        },
        "ifTrueActivities": [/* process data */],
        "ifFalseActivities": [/* handle no data */]
    }
}
```

**2. Use Specific Queries:**
```sql
-- Bad: Return all columns
SELECT * FROM LargeTable

-- Good: Return only needed columns
SELECT ConfigKey, ConfigValue FROM Config WHERE Active = 1
```

**3. Limit Rows:**
```sql
-- Add TOP/LIMIT to prevent large results
SELECT TOP 1000 * FROM FileList
```

**4. Index Lookup Columns:**
```sql
-- Create index on frequently looked-up columns
CREATE INDEX IX_Config_Environment ON Config(Environment)
```

---

## 60. How do you implement incremental load in ADF?

### Incremental Load Strategy

**Definition:** Load only new or changed data since the last load, instead of reloading all data.

### Implementation Methods

#### **Method 1: Watermark Column (Most Common)**

**Using LastModifiedDate:**

```json
// Step 1: Lookup old watermark
{
    "name": "GetOldWatermark",
    "type": "Lookup",
    "typeProperties": {
        "source": {
            "type": "AzureSqlSource",
            "sqlReaderQuery": "SELECT MAX(WatermarkValue) as WatermarkValue FROM Watermark WHERE TableName = 'Orders'"
        },
        "dataset": {"referenceName": "WatermarkTable"}
    }
}

// Step 2: Lookup new watermark (from source)
{
    "name": "GetNewWatermark",
    "type": "Lookup",
    "typeProperties": {
        "source": {
            "type": "AzureSqlSource",
            "sqlReaderQuery": "SELECT MAX(LastModifiedDate) as NewWatermarkValue FROM Orders"
        },
        "dataset": {"referenceName": "SourceDB"}
    }
}

// Step 3: Copy incremental data
{
    "name": "IncrementalCopy",
    "type": "Copy",
    "dependsOn": [
        {"activity": "GetOldWatermark"},
        {"activity": "GetNewWatermark"}
    ],
    "typeProperties": {
        "source": {
            "type": "SqlSource",
            "sqlReaderQuery": {
                "value": "SELECT * FROM Orders WHERE LastModifiedDate > '@{activity('GetOldWatermark').output.firstRow.WatermarkValue}' AND LastModifiedDate <= '@{activity('GetNewWatermark').output.firstRow.NewWatermarkValue}'",
                "type": "Expression"
            }
        },
        "sink": {"type": "ParquetSink"}
    }
}

// Step 4: Update watermark
{
    "name": "UpdateWatermark",
    "type": "SqlServerStoredProcedure",
    "dependsOn": [{"activity": "IncrementalCopy"}],
    "typeProperties": {
        "storedProcedureName": "usp_write_watermark",
        "storedProcedureParameters": {
            "TableName": "Orders",
            "WatermarkValue": "@activity('GetNewWatermark').output.firstRow.NewWatermarkValue"
        }
    }
}
```

#### **Method 2: Change Data Capture (CDC)**

**Using SQL Server CDC:**

```sql
-- Enable CDC on source table
USE SourceDB;
EXEC sys.sp_cdc_enable_table
    @source_schema = 'dbo',
    @source_name = 'Orders',
    @role_name = NULL;
```

```json
// Copy CDC changes
{
    "name": "CopyCDCChanges",
    "type": "Copy",
    "typeProperties": {
        "source": {
            "type": "SqlSource",
            "sqlReaderQuery": "SELECT * FROM cdc.fn_cdc_get_all_changes_dbo_Orders(@from_lsn, @to_lsn, 'all')"
        },
        "sink": {"type": "ParquetSink"}
    }
}
```

#### **Method 3: Delta Lake (Change Feed)**

**Using Azure Data Lake:**

```json
{
    "source": {
        "type": "DelimitedTextSource",
        "storeSettings": {
            "type": "AzureBlobFSReadSettings",
            "modifiedDatetimeStart": "@activity('GetLastRunTime').output.firstRow.LastRunTime",
            "modifiedDatetimeEnd": "@utcnow()"
        }
    }
}
```

---

## 61. How do you update the watermark column during incremental load?

### Watermark Update Pattern

**Complete Pattern:**

```json
// Pipeline: Incremental Load with Watermark

// Activity 1: Get Last Watermark
{
    "name": "LookupOldWatermarkActivity",
    "type": "Lookup",
    "typeProperties": {
        "source": {
            "type": "AzureSqlSource",
            "sqlReaderQuery": "SELECT WatermarkValue FROM watermarktable WHERE TableName = '@{pipeline().parameters.TableName}'"
        },
        "dataset": {"referenceName": "WatermarkDataset"}
    }
}

// Activity 2: Get Current Max Value
{
    "name": "LookupNewWatermarkActivity",
    "type": "Lookup",
    "typeProperties": {
        "source": {
            "type": "AzureSqlSource",
            "sqlReaderQuery": {
                "value": "SELECT MAX(@{pipeline().parameters.WatermarkColumn}) as NewWatermarkValue FROM @{pipeline().parameters.TableName}",
                "type": "Expression"
            }
        },
        "dataset": {"referenceName": "SourceDataset"}
    }
}

// Activity 3: Copy Incremental Data
{
    "name": "IncrementalCopyActivity",
    "type": "Copy",
    "inputs": [{"referenceName": "SourceDataset"}],
    "outputs": [{"referenceName": "SinkDataset"}],
    "typeProperties": {
        "source": {
            "type": "SqlSource",
            "sqlReaderQuery": {
                "value": "SELECT * FROM @{pipeline().parameters.TableName} WHERE @{pipeline().parameters.WatermarkColumn} > '@{activity('LookupOldWatermarkActivity').output.firstRow.WatermarkValue}' AND @{pipeline().parameters.WatermarkColumn} <= '@{activity('LookupNewWatermarkActivity').output.firstRow.NewWatermarkValue}'",
                "type": "Expression"
            }
        }
    }
}

// Activity 4: Stored Procedure to Update Watermark
{
    "name": "StoredProceduretoWriteWatermarkActivity",
    "type": "SqlServerStoredProcedure",
    "dependsOn": [{"activity": "IncrementalCopyActivity", "dependencyConditions": ["Succeeded"]}],
    "typeProperties": {
        "storedProcedureName": "usp_write_watermark",
        "storedProcedureParameters": {
            "LastModifiedtime": {
                "value": "@activity('LookupNewWatermarkActivity').output.firstRow.NewWatermarkValue",
                "type": "DateTime"
            },
            "TableName": {
                "value": "@pipeline().parameters.TableName",
                "type": "String"
            }
        }
    },
    "linkedServiceName": {"referenceName": "WatermarkDB"}
}
```

**Stored Procedure:**

```sql
CREATE PROCEDURE usp_write_watermark
    @LastModifiedtime DATETIME,
    @TableName VARCHAR(50)
AS
BEGIN
    UPDATE watermarktable
    SET WatermarkValue = @LastModifiedtime
    WHERE TableName = @TableName;
    
    IF @@ROWCOUNT = 0
        INSERT INTO watermarktable VALUES (@TableName, @LastModifiedtime);
END
```

---

## 62. How do you send notifications from ADF pipelines?

### Notification Methods

#### **Method 1: Logic Apps (Recommended)**

```json
{
    "name": "SendEmailNotification",
    "type": "WebActivity",
    "dependsOn": [{"activity": "CopyActivity"}],
    "typeProperties": {
        "url": "https://<region>.logic.azure.com/workflows/<workflow-id>/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=<signature>",
        "method": "POST",
        "headers": {
            "Content-Type": "application/json"
        },
        "body": {
            "dataFactoryName": "@pipeline().DataFactory",
            "pipelineName": "@pipeline().Pipeline",
            "runId": "@pipeline().RunId",
            "status": "Success",
            "message": "Pipeline completed successfully",
            "rowsCopied": "@activity('CopyActivity').output.rowsCopied"
        }
    }
}
```

#### **Method 2: Azure Function**

```json
{
    "name": "CallAzureFunction",
    "type": "AzureFunctionActivity",
    "typeProperties": {
        "functionName": "SendNotification",
        "method": "POST",
        "body": {
            "pipelineName": "@pipeline().Pipeline",
            "status": "@activity('CopyActivity').Status",
            "error": "@activity('CopyActivity').Error"
        }
    },
    "linkedServiceName": {"referenceName": "AzureFunctionLS"}
}
```

#### **Method 3: Email via Graph API**

```json
{
    "name": "SendEmail",
    "type": "WebActivity",
    "typeProperties": {
        "url": "https://graph.microsoft.com/v1.0/me/sendMail",
        "method": "POST",
        "headers": {
            "Authorization": "Bearer @{activity('GetToken').output.access_token}",
            "Content-Type": "application/json"
        },
        "body": {
            "message": {
                "subject": "ADF Pipeline Notification",
                "body": {
                    "contentType": "Text",
                    "content": "Pipeline @{pipeline().Pipeline} completed"
                },
                "toRecipients": [
                    {"emailAddress": {"address": "team@company.com"}}
                ]
            }
        }
    }
}
```

---

## 63. How do you handle a pipeline failure that occurs in the middle?

### Error Handling Strategies

#### **1. Activity-Level Retry**

```json
{
    "name": "CopyActivity",
    "type": "Copy",
    "policy": {
        "retry": 3,
        "retryIntervalInSeconds": 30,
        "secureOutput": false,
        "secureInput": false,
        "timeout": "7.00:00:00"
    }
}
```

#### **2. Dependency Conditions**

```json
{
    "name": "CleanupOnFailure",
    "type": "Delete",
    "dependsOn": [
        {
            "activity": "CopyActivity",
            "dependencyConditions": ["Failed"]
        }
    ]
}

{
    "name": "SendSuccessEmail",
    "type": "WebActivity",
    "dependsOn": [
        {
            "activity": "CopyActivity",
            "dependencyConditions": ["Succeeded"]
        }
    ]
}

{
    "name": "AlwaysRun",
    "type": "WebActivity",
    "dependsOn": [
        {
            "activity": "CopyActivity",
            "dependencyConditions": ["Succeeded", "Failed", "Skipped", "Completed"]
        }
    ]
}
```

#### **3. Try-Catch Pattern (Until Activity)**

```json
{
    "name": "TryCatchPattern",
    "type": "Until",
    "typeProperties": {
        "expression": {
            "value": "@or(equals(variables('Success'), true), greaterOrEquals(variables('RetryCount'), 3))",
            "type": "Expression"
        },
        "activities": [
            {
                "name": "TryOperation",
                "type": "Copy",
                // ... copy settings
            },
            {
                "name": "SetSuccess",
                "type": "SetVariable",
                "dependsOn": [
                    {
                        "activity": "TryOperation",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "typeProperties": {
                    "variableName": "Success",
                    "value": true
                }
            },
            {
                "name": "IncrementRetry",
                "type": "SetVariable",
                "dependsOn": [
                    {
                        "activity": "TryOperation",
                        "dependencyConditions": ["Failed"]
                    }
                ],
                "typeProperties": {
                    "variableName": "RetryCount",
                    "value": "@add(variables('RetryCount'), 1)"
                }
            }
        ],
        "timeout": "7.00:00:00"
    }
}
```

#### **4. Checkpoint Pattern**

```sql
-- Checkpoint table
CREATE TABLE ETL_Checkpoint (
    PipelineRunId VARCHAR(100),
    ActivityName VARCHAR(100),
    Status VARCHAR(20),
    LastUpdated DATETIME
);

-- Update checkpoint after each major activity
```

```json
{
    "name": "UpdateCheckpoint",
    "type": "SqlServerStoredProcedure",
    "typeProperties": {
        "storedProcedureName": "usp_UpdateCheckpoint",
        "storedProcedureParameters": {
            "RunId": "@pipeline().RunId",
            "ActivityName": "CopyData",
            "Status": "Completed"
        }
    }
}
```

---

## 64-72. Rapid Fire ADF Questions

### 64. How would you optimize a 100GB data pipeline in ADF?

**Optimizations:**
1. **Increase DIUs:** Set to 32 or 64
2. **Enable Staging:** For cross-region copies
3. **Partitioning:** Use parallel copy with partitioning
4. **Compression:** Enable compression for staging
5. **File Format:** Use Parquet instead of CSV
6. **Batch Size:** Optimize write batch size (10K-50K rows)

### 65. What are Mapping Data Flow and Wrangling Data Flow and how do they differ?

| Feature | Mapping Data Flow | Wrangling Data Flow |
|---------|------------------|---------------------|
| **Engine** | Spark-based | Power Query engine |
| **Use Case** | Complex transformations | Simple data prep |
| **Code** | Visual (no code) | Visual (Power Query) |
| **Scale** | Large datasets (TB) | Small/Medium (GB) |
| **Performance** | High (distributed) | Lower (single node) |

### 66. What are Web activity and Webhook activity in ADF?

**Web Activity:**
- Call REST APIs
- Synchronous (waits for response)
- Timeout limit

```json
{
    "name": "CallAPI",
    "type": "WebActivity",
    "typeProperties": {
        "url": "https://api.example.com/data",
        "method": "GET",
        "headers": {"Authorization": "Bearer token"}
    }
}
```

**Webhook Activity:**
- Call external service
- Asynchronous (callback pattern)
- Service calls back to ADF when done

```json
{
    "name": "WebhookActivity",
    "type": "WebHook",
    "typeProperties": {
        "url": "https://service.example.com/process",
        "method": "POST",
        "body": {"data": "value"},
        "timeout": "01:00:00"
    }
}
```

### 67. What are triggers in ADF and how many types of triggers are there?

**Three Types:**

1. **Schedule Trigger:** Time-based
2. **Tumbling Window Trigger:** Time windows with dependencies
3. **Event-based Trigger:** Storage events (blob created/deleted)

### 68. How do you design an ADF pipeline when the source file is a ZIP file?

**Solution:**

```json
// 1. Copy ZIP to staging
{
    "name": "CopyZipFile",
    "type": "Copy",
    "typeProperties": {
        "source": {"type": "BinarySource"},
        "sink": {"type": "BinarySink"}
    }
}

// 2. Execute Azure Function to unzip
{
    "name": "UnzipFile",
    "type": "AzureFunctionActivity",
    "typeProperties": {
        "functionName": "UnzipFunction",
        "method": "POST",
        "body": {
            "zipFilePath": "@activity('CopyZipFile').output.fileName"
        }
    }
}

// 3. Process unzipped files
{
    "name": "ProcessFiles",
    "type": "ForEach",
    "typeProperties": {
        "items": "@activity('UnzipFile').output.files"
    }
}
```

### 69. How do you connect Azure Data Factory to Databricks?

**Steps:**

1. **Create Databricks Linked Service:**

```json
{
    "name": "DatabricksLinkedService",
    "type": "AzureDatabricks",
    "typeProperties": {
        "domain": "https://adb-12345.azuredatabricks.net",
        "accessToken": {
            "type": "AzureKeyVaultSecret",
            "store": {"referenceName": "AzureKeyVault"},
            "secretName": "databricks-token"
        },
        "existingClusterId": "1234-567890-abc123"
    }
}
```

2. **Use Databricks Notebook Activity:**

```json
{
    "name": "RunNotebook",
    "type": "DatabricksNotebook",
    "linkedServiceName": {"referenceName": "DatabricksLinkedService"},
    "typeProperties": {
        "notebookPath": "/Users/me/MyNotebook",
        "baseParameters": {
            "input_path": "@pipeline().parameters.InputPath",
            "date": "@formatDateTime(utcnow(), 'yyyy-MM-dd')"
        }
    }
}
```

### 70. How do you handle errors in ADF and where do you store logs?

**Error Handling:**

1. **Activity Policy:** Retry logic
2. **Dependency Conditions:** Conditional execution
3. **Monitoring:** Azure Monitor
4. **Alerts:** Email/webhook notifications

**Log Storage:**

```
1. Azure Monitor Logs
   - Pipeline runs
   - Activity runs
   - Trigger runs

2. Log Analytics Workspace
   - Query with KQL
   - Custom dashboards

3. Application Insights
   - Detailed diagnostics

4. Custom Logging
   - Log to SQL table
   - Log to Blob storage
```

### 71. How do you confirm the file format in an ADF pipeline?

**Solution:**

```json
// Use Get Metadata Activity
{
    "name": "GetFileMetadata",
    "type": "GetMetadata",
    "typeProperties": {
        "dataset": {"referenceName": "InputFile"},
        "fieldList": [
            "itemName",
            "itemType",
            "size",
            "structure"  // Gets column structure
        ]
    }
}

// Validate format
{
    "name": "ValidateFormat",
    "type": "IfCondition",
    "dependsOn": [{"activity": "GetFileMetadata"}],
    "typeProperties": {
        "expression": {
            "value": "@endswith(activity('GetFileMetadata').output.itemName, '.csv')",
            "type": "Expression"
        },
        "ifTrueActivities": [/* Process */],
        "ifFalseActivities": [/* Reject */]
    }
}
```

### 72. You have JSON and CSV files in Blob Storage. How do you move data from Blob to ADLS using ADF?

**Solution:**

```json
// Get file list with metadata
{
    "name": "GetFileList",
    "type": "GetMetadata",
    "typeProperties": {
        "dataset": {"referenceName": "BlobStorage"},
        "fieldList": ["childItems"]
    }
}

// ForEach file
{
    "name": "ProcessEachFile",
    "type": "ForEach",
    "typeProperties": {
        "items": "@activity('GetFileList').output.childItems",
        "activities": [
            // Check file type
            {
                "name": "CheckFileType",
                "type": "IfCondition",
                "typeProperties": {
                    "expression": {
                        "value": "@or(endswith(item().name, '.json'), endswith(item().name, '.csv'))",
                        "type": "Expression"
                    },
                    "ifTrueActivities": [
                        {
                            "name": "CopyFile",
                            "type": "Copy",
                            "inputs": [{
                                "referenceName": "BlobSource",
                                "parameters": {"fileName": "@item().name"}
                            }],
                            "outputs": [{
                                "referenceName": "ADLSSink",
                                "parameters": {"fileName": "@item().name"}
                            }]
                        }
                    ]
                }
            }
        ]
    }
}
```

---

*End of Azure Data Factory Guide*

## Summary

This guide covered 18 comprehensive questions on Azure Data Factory:
1. What is Azure Data Factory?
2. Integration Runtime types
3. Copy Data activity and batch count
4. ForEach activity and batch count
5. Lookup activity
6. Incremental load implementation
7. Watermark column updates
8. Sending notifications
9. Handling pipeline failures
10-18. Additional topics: optimization, data flows, web/webhook activities, triggers, ZIP files, Databricks connection, error handling, file format validation, and multi-format file processing

Each question includes detailed explanations, JSON examples, real-world scenarios, and best practices.
