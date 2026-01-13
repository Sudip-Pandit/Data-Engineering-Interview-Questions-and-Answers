# SQL Practical & Theory Interview Guide

## 86. Write a SQL query to find the second highest salary from an Employee table

### Method 1: Using LIMIT/OFFSET (MySQL, PostgreSQL)

```sql
SELECT DISTINCT salary
FROM employees
ORDER BY salary DESC
LIMIT 1 OFFSET 1;
```

### Method 2: Using Subquery

```sql
SELECT MAX(salary) as second_highest_salary
FROM employees
WHERE salary < (SELECT MAX(salary) FROM employees);
```

### Method 3: Using DENSE_RANK() (All Databases)

```sql
SELECT salary as second_highest_salary
FROM (
    SELECT salary, DENSE_RANK() OVER (ORDER BY salary DESC) as rank
    FROM employees
) ranked
WHERE rank = 2;
```

### Method 4: Using ROW_NUMBER()

```sql
SELECT salary as second_highest_salary
FROM (
    SELECT DISTINCT salary, ROW_NUMBER() OVER (ORDER BY salary DESC) as rn
    FROM employees
) ranked
WHERE rn = 2;
```

### Method 5: Using TOP (SQL Server)

```sql
SELECT TOP 1 salary as second_highest_salary
FROM (
    SELECT DISTINCT TOP 2 salary
    FROM employees
    ORDER BY salary DESC
) top_salaries
ORDER BY salary ASC;
```

### Handling NULL Cases

```sql
-- If no second highest exists, return NULL
SELECT 
    CASE 
        WHEN COUNT(DISTINCT salary) >= 2 
        THEN (
            SELECT salary 
            FROM (
                SELECT DISTINCT salary, DENSE_RANK() OVER (ORDER BY salary DESC) as rank
                FROM employees
            ) ranked
            WHERE rank = 2
        )
        ELSE NULL
    END as second_highest_salary
FROM employees;
```

### Generic: Nth Highest Salary

```sql
-- Find Nth highest salary (N = 3 for third highest)
DECLARE @N INT = 3;

SELECT salary
FROM (
    SELECT DISTINCT salary, DENSE_RANK() OVER (ORDER BY salary DESC) as rank
    FROM employees
) ranked
WHERE rank = @N;
```

### Complete Example with Test Data

```sql
-- Create sample table
CREATE TABLE employees (
    employee_id INT,
    name VARCHAR(100),
    salary DECIMAL(10,2)
);

-- Insert test data
INSERT INTO employees VALUES
(1, 'Alice', 100000),
(2, 'Bob', 90000),
(3, 'Charlie', 90000),  -- Duplicate salary
(4, 'David', 80000),
(5, 'Eve', 75000);

-- Find second highest (should return 90000)
SELECT salary as second_highest_salary
FROM (
    SELECT DISTINCT salary, DENSE_RANK() OVER (ORDER BY salary DESC) as rank
    FROM employees
) ranked
WHERE rank = 2;

-- Result: 90000
```

### Comparison of Methods

| Method | Pros | Cons |
|--------|------|------|
| LIMIT/OFFSET | Simple, concise | Not standard SQL, doesn't handle ties |
| MAX with subquery | Easy to understand | Inefficient for large tables |
| DENSE_RANK | Handles duplicates, portable | Requires window function support |
| ROW_NUMBER | Simple window function | Doesn't handle ties well |
| TOP | Works in SQL Server | Not portable |

### Best Practice Recommendation

**Use DENSE_RANK()** - It's portable, handles duplicates correctly, and is efficient:

```sql
SELECT salary as second_highest_salary
FROM (
    SELECT salary, DENSE_RANK() OVER (ORDER BY salary DESC) as rank
    FROM employees
) ranked
WHERE rank = 2;
```

---

## 87. Write a SQL query to find duplicate records in a table

### Method 1: Using GROUP BY and HAVING

```sql
-- Find duplicate emails
SELECT email, COUNT(*) as count
FROM customers
GROUP BY email
HAVING COUNT(*) > 1;
```

### Method 2: Find All Duplicate Rows (Complete Records)

```sql
-- Get all rows that have duplicates
SELECT *
FROM customers c1
WHERE EXISTS (
    SELECT 1
    FROM customers c2
    WHERE c1.email = c2.email
      AND c1.customer_id != c2.customer_id
);
```

### Method 3: Using Window Functions

```sql
-- Mark duplicates with row numbers
SELECT *
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY email ORDER BY customer_id) as rn
    FROM customers
) ranked
WHERE rn > 1;
-- Returns all duplicate rows except the first occurrence
```

### Method 4: Find Duplicates Across Multiple Columns

```sql
-- Find duplicates based on name AND email
SELECT name, email, COUNT(*) as duplicate_count
FROM customers
GROUP BY name, email
HAVING COUNT(*) > 1;
```

### Method 5: Get Complete Duplicate Records with Details

```sql
-- Show all information about duplicates
SELECT c.*
FROM customers c
INNER JOIN (
    SELECT email
    FROM customers
    GROUP BY email
    HAVING COUNT(*) > 1
) duplicates ON c.email = duplicates.email
ORDER BY c.email, c.customer_id;
```

### Method 6: Find Exact Duplicate Rows (All Columns)

```sql
-- Find rows that are completely identical
SELECT 
    name,
    email,
    phone,
    address,
    COUNT(*) as occurrence_count
FROM customers
GROUP BY name, email, phone, address
HAVING COUNT(*) > 1;
```

### Real-world Example: Customer Deduplication

```sql
-- Sample data with duplicates
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    registration_date DATE
);

INSERT INTO customers VALUES
(1, 'John Doe', 'john@example.com', '555-1234', '2024-01-01'),
(2, 'Jane Smith', 'jane@example.com', '555-5678', '2024-01-02'),
(3, 'John Doe', 'john@example.com', '555-1234', '2024-01-03'),  -- Duplicate
(4, 'Bob Johnson', 'bob@example.com', '555-9012', '2024-01-04'),
(5, 'John Doe', 'john@example.com', '555-1234', '2024-01-05');  -- Duplicate

-- Find duplicates by email
SELECT 
    email,
    COUNT(*) as duplicate_count,
    MIN(customer_id) as first_id,
    MAX(customer_id) as last_id,
    STRING_AGG(CAST(customer_id AS VARCHAR), ',') as all_ids
FROM customers
GROUP BY email
HAVING COUNT(*) > 1;

-- Result:
-- email              | duplicate_count | first_id | last_id | all_ids
-- john@example.com   | 3               | 1        | 5       | 1,3,5
```

### Removing Duplicates

#### **Option 1: Delete All But First Occurrence**

```sql
-- Using CTE with ROW_NUMBER
WITH ranked AS (
    SELECT 
        customer_id,
        ROW_NUMBER() OVER (PARTITION BY email ORDER BY customer_id) as rn
    FROM customers
)
DELETE FROM customers
WHERE customer_id IN (
    SELECT customer_id 
    FROM ranked 
    WHERE rn > 1
);
```

#### **Option 2: Keep Newest Record**

```sql
WITH ranked AS (
    SELECT 
        customer_id,
        ROW_NUMBER() OVER (
            PARTITION BY email 
            ORDER BY registration_date DESC, customer_id DESC
        ) as rn
    FROM customers
)
DELETE FROM customers
WHERE customer_id IN (
    SELECT customer_id 
    FROM ranked 
    WHERE rn > 1
);
```

#### **Option 3: Create Deduplicated Table**

```sql
-- Create new table without duplicates
CREATE TABLE customers_deduplicated AS
SELECT DISTINCT ON (email)
    customer_id,
    name,
    email,
    phone,
    registration_date
FROM customers
ORDER BY email, customer_id;
-- PostgreSQL syntax

-- SQL Server equivalent
SELECT *
INTO customers_deduplicated
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY email ORDER BY customer_id) as rn
    FROM customers
) ranked
WHERE rn = 1;
```

### Preventing Duplicates

```sql
-- Add unique constraint
ALTER TABLE customers
ADD CONSTRAINT uq_email UNIQUE (email);

-- Composite unique constraint
ALTER TABLE customers
ADD CONSTRAINT uq_name_email UNIQUE (name, email);

-- Unique index
CREATE UNIQUE INDEX idx_email ON customers(email);
```

---

## 88. Write a SQL query to find employees who earn more than their managers

```sql
-- Assuming employees table with manager_id referencing employee_id
SELECT 
    e.employee_id,
    e.name as employee_name,
    e.salary as employee_salary,
    m.name as manager_name,
    m.salary as manager_salary
FROM employees e
INNER JOIN employees m ON e.manager_id = m.employee_id
WHERE e.salary > m.salary;
```

### Complete Example

```sql
-- Create employees table
CREATE TABLE employees (
    employee_id INT PRIMARY KEY,
    name VARCHAR(100),
    salary DECIMAL(10,2),
    manager_id INT
);

-- Insert sample data
INSERT INTO employees VALUES
(1, 'Alice', 150000, NULL),      -- CEO (no manager)
(2, 'Bob', 120000, 1),           -- Reports to Alice
(3, 'Charlie', 130000, 1),       -- Reports to Alice, earns more than Bob
(4, 'David', 90000, 2),          -- Reports to Bob
(5, 'Eve', 125000, 2),           -- Reports to Bob, earns more than Bob!
(6, 'Frank', 95000, 3);          -- Reports to Charlie

-- Find employees earning more than their managers
SELECT 
    e.employee_id,
    e.name as employee_name,
    e.salary as employee_salary,
    m.name as manager_name,
    m.salary as manager_salary,
    e.salary - m.salary as salary_difference
FROM employees e
INNER JOIN employees m ON e.manager_id = m.employee_id
WHERE e.salary > m.salary;

-- Result:
-- employee_id | employee_name | employee_salary | manager_name | manager_salary | salary_difference
-- 3           | Charlie       | 130000          | Alice        | 150000         | -20000  (Wait, this is wrong!)
-- 5           | Eve           | 125000          | Bob          | 120000         | 5000

-- Correct query (Charlie doesn't earn more than Alice):
-- employee_id | employee_name | employee_salary | manager_name | manager_salary | salary_difference
-- 5           | Eve           | 125000          | Bob          | 120000         | 5000
```

### Variations

**Include Percentage Difference:**
```sql
SELECT 
    e.name as employee_name,
    e.salary as employee_salary,
    m.name as manager_name,
    m.salary as manager_salary,
    ROUND((e.salary - m.salary) * 100.0 / m.salary, 2) as pct_more_than_manager
FROM employees e
INNER JOIN employees m ON e.manager_id = m.employee_id
WHERE e.salary > m.salary
ORDER BY pct_more_than_manager DESC;
```

**Include Department:**
```sql
SELECT 
    e.name as employee_name,
    e.salary as employee_salary,
    e.department,
    m.name as manager_name,
    m.salary as manager_salary
FROM employees e
INNER JOIN employees m ON e.manager_id = m.employee_id
WHERE e.salary > m.salary
ORDER BY e.department, e.salary DESC;
```

---

## 89. Write a SQL query to find the nth highest salary without using TOP/LIMIT

### Using DENSE_RANK()

```sql
-- Find 3rd highest salary
SELECT salary
FROM (
    SELECT 
        salary,
        DENSE_RANK() OVER (ORDER BY salary DESC) as rank
    FROM employees
) ranked
WHERE rank = 3;
```

### Using Correlated Subquery

```sql
-- Find 3rd highest salary
SELECT DISTINCT salary
FROM employees e1
WHERE 3 = (
    SELECT COUNT(DISTINCT salary)
    FROM employees e2
    WHERE e2.salary >= e1.salary
);
```

### Using Self-Join

```sql
-- Find 3rd highest salary
SELECT DISTINCT e1.salary
FROM employees e1
WHERE (
    SELECT COUNT(DISTINCT e2.salary)
    FROM employees e2
    WHERE e2.salary > e1.salary
) = 2;  -- 2 salaries higher means this is 3rd
```

### Generic Function for Nth Highest

```sql
-- SQL Server function
CREATE FUNCTION dbo.GetNthHighestSalary(@N INT)
RETURNS DECIMAL(10,2)
AS
BEGIN
    DECLARE @result DECIMAL(10,2);
    
    SELECT @result = salary
    FROM (
        SELECT 
            salary,
            DENSE_RANK() OVER (ORDER BY salary DESC) as rank
        FROM employees
    ) ranked
    WHERE rank = @N;
    
    RETURN @result;
END;

-- Usage
SELECT dbo.GetNthHighestSalary(3) as third_highest_salary;
```

---

## 90. Write a SQL query to delete duplicate rows keeping one

### Method 1: Using CTE with ROW_NUMBER (Recommended)

```sql
WITH ranked AS (
    SELECT 
        customer_id,
        ROW_NUMBER() OVER (
            PARTITION BY email 
            ORDER BY customer_id  -- Keep the one with smallest ID
        ) as rn
    FROM customers
)
DELETE FROM customers
WHERE customer_id IN (
    SELECT customer_id FROM ranked WHERE rn > 1
);
```

### Method 2: Using Self-Join

```sql
DELETE c1
FROM customers c1
INNER JOIN customers c2 
    ON c1.email = c2.email 
    AND c1.customer_id > c2.customer_id;
-- Deletes all duplicates except the one with smallest customer_id
```

### Method 3: Using Temporary Table

```sql
-- Step 1: Create temp table with deduplicated data
SELECT DISTINCT ON (email)
    customer_id,
    name,
    email,
    phone
INTO customers_temp
FROM customers
ORDER BY email, customer_id;

-- Step 2: Drop original table
DROP TABLE customers;

-- Step 3: Rename temp table
ALTER TABLE customers_temp RENAME TO customers;
```

### Method 4: Keep Most Recent Record

```sql
WITH ranked AS (
    SELECT 
        customer_id,
        ROW_NUMBER() OVER (
            PARTITION BY email 
            ORDER BY created_date DESC  -- Keep newest
        ) as rn
    FROM customers
)
DELETE FROM customers
WHERE customer_id IN (
    SELECT customer_id FROM ranked WHERE rn > 1
);
```

### Complete Example

```sql
-- Sample data with duplicates
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    created_date DATE
);

INSERT INTO customers VALUES
(1, 'John Doe', 'john@example.com', '2024-01-01'),
(2, 'Jane Smith', 'jane@example.com', '2024-01-02'),
(3, 'John Doe', 'john@example.com', '2024-01-03'),  -- Duplicate
(4, 'John Doe', 'john@example.com', '2024-01-05'),  -- Duplicate (newest)
(5, 'Bob Johnson', 'bob@example.com', '2024-01-04');

-- Before deletion: 5 rows, 3 with john@example.com

-- Delete duplicates, keep newest
WITH ranked AS (
    SELECT 
        customer_id,
        ROW_NUMBER() OVER (
            PARTITION BY email 
            ORDER BY created_date DESC
        ) as rn
    FROM customers
)
DELETE FROM customers
WHERE customer_id IN (
    SELECT customer_id FROM ranked WHERE rn > 1
);

-- After deletion: 3 rows
-- Remaining: customer_id 2, 4, 5 (kept newest John Doe with id=4)
```

---

## 91. Explain the difference between UNION and UNION ALL

### UNION

**Definition:** Combines result sets and removes duplicates.

```sql
SELECT customer_id, name FROM customers_usa
UNION
SELECT customer_id, name FROM customers_uk;

-- Result: Unique rows only (duplicates removed)
-- Performance: Slower (requires sorting/deduplication)
```

### UNION ALL

**Definition:** Combines result sets and keeps all rows including duplicates.

```sql
SELECT customer_id, name FROM customers_usa
UNION ALL
SELECT customer_id, name FROM customers_uk;

-- Result: All rows (duplicates included)
-- Performance: Faster (no deduplication needed)
```

### Comparison Table

| Aspect | UNION | UNION ALL |
|--------|-------|-----------|
| **Duplicates** | Removed | Kept |
| **Performance** | Slower | Faster |
| **Sorting** | Implicit sort | No sorting |
| **Use Case** | Need unique results | All rows needed |
| **Memory** | Higher (dedup) | Lower |

### Example

```sql
-- Create sample tables
CREATE TABLE sales_2023 (
    product VARCHAR(50),
    amount DECIMAL(10,2)
);

CREATE TABLE sales_2024 (
    product VARCHAR(50),
    amount DECIMAL(10,2)
);

INSERT INTO sales_2023 VALUES
('Laptop', 1000),
('Mouse', 25),
('Keyboard', 75);

INSERT INTO sales_2024 VALUES
('Laptop', 1100),  -- Duplicate product
('Monitor', 300),
('Mouse', 25);     -- Duplicate product and amount

-- UNION (removes duplicates)
SELECT product, amount FROM sales_2023
UNION
SELECT product, amount FROM sales_2024;

-- Result: 5 rows (Mouse $25 appears once)
-- Laptop  1000
-- Mouse   25
-- Keyboard 75
-- Laptop  1100
-- Monitor 300

-- UNION ALL (keeps duplicates)
SELECT product, amount FROM sales_2023
UNION ALL
SELECT product, amount FROM sales_2024;

-- Result: 6 rows (Mouse $25 appears twice)
-- Laptop  1000
-- Mouse   25
-- Keyboard 75
-- Laptop  1100
-- Monitor 300
-- Mouse   25
```

### When to Use Each

**Use UNION when:**
- You need unique results
- Removing duplicates is important
- Data integrity requires uniqueness

**Use UNION ALL when:**
- You need all rows
- You know there are no duplicates
- Performance is critical
- Duplicates are meaningful (e.g., transaction logs)

### Performance Comparison

```sql
-- Slow (removes duplicates)
SELECT customer_id FROM orders_2023
UNION
SELECT customer_id FROM orders_2024;

-- Fast (keeps all)
SELECT customer_id FROM orders_2023
UNION ALL
SELECT customer_id FROM orders_2024;

-- If you need unique results but know there are no duplicates:
-- Still use UNION ALL for better performance
```

---

## 92. What is the difference between WHERE and HAVING?

### WHERE Clause

**Purpose:** Filters rows BEFORE grouping.

**Used with:** Individual rows

**Can use:** Column names, not aggregate functions

```sql
SELECT department, AVG(salary)
FROM employees
WHERE salary > 50000  -- Filter rows before grouping
GROUP BY department;
```

### HAVING Clause

**Purpose:** Filters groups AFTER grouping.

**Used with:** Grouped results

**Can use:** Aggregate functions

```sql
SELECT department, AVG(salary) as avg_salary
FROM employees
GROUP BY department
HAVING AVG(salary) > 80000;  -- Filter groups after aggregation
```

### Comparison Table

| Aspect | WHERE | HAVING |
|--------|-------|--------|
| **When Applied** | Before GROUP BY | After GROUP BY |
| **Filters** | Individual rows | Groups |
| **Aggregates** | Cannot use | Can use |
| **Performance** | Faster (reduces rows early) | Slower (processes all first) |
| **Example** | WHERE salary > 50000 | HAVING AVG(salary) > 50000 |

### Examples

#### **Example 1: WHERE Only**

```sql
-- Get employees in IT department earning > $70K
SELECT name, salary
FROM employees
WHERE department = 'IT' 
  AND salary > 70000;
```

#### **Example 2: HAVING Only**

```sql
-- Get departments with average salary > $80K
SELECT department, AVG(salary) as avg_salary
FROM employees
GROUP BY department
HAVING AVG(salary) > 80000;
```

#### **Example 3: WHERE and HAVING Together**

```sql
-- Get departments with > 5 employees earning over $50K
-- where the average salary of those employees > $70K
SELECT 
    department,
    COUNT(*) as employee_count,
    AVG(salary) as avg_salary
FROM employees
WHERE salary > 50000        -- Filter rows first
GROUP BY department
HAVING COUNT(*) > 5         -- Filter groups
   AND AVG(salary) > 70000;
```

### Complete Example

```sql
-- Sample data
CREATE TABLE employees (
    employee_id INT,
    name VARCHAR(100),
    department VARCHAR(50),
    salary DECIMAL(10,2)
);

INSERT INTO employees VALUES
(1, 'Alice', 'IT', 90000),
(2, 'Bob', 'IT', 85000),
(3, 'Charlie', 'IT', 95000),
(4, 'David', 'Sales', 60000),
(5, 'Eve', 'Sales', 65000),
(6, 'Frank', 'Sales', 70000),
(7, 'Grace', 'HR', 55000),
(8, 'Henry', 'HR', 58000);

-- Query 1: WHERE only
SELECT name, salary
FROM employees
WHERE salary > 70000;
-- Result: Alice, Bob, Charlie, Frank (4 rows)

-- Query 2: HAVING only
SELECT department, AVG(salary) as avg_salary
FROM employees
GROUP BY department
HAVING AVG(salary) > 65000;
-- Result: IT (90000), Sales (65000) - 2 groups

-- Query 3: WHERE + HAVING
SELECT 
    department,
    COUNT(*) as high_earners,
    AVG(salary) as avg_salary
FROM employees
WHERE salary > 60000          -- Only include employees earning > 60K
GROUP BY department
HAVING COUNT(*) >= 2;         -- Only departments with 2+ such employees
-- Result: IT (3 employees, avg 90000), Sales (2 employees, avg 67500)
```

### Common Mistake

```sql
-- WRONG: Can't use aggregate in WHERE
SELECT department, AVG(salary)
FROM employees
WHERE AVG(salary) > 80000  -- ❌ ERROR!
GROUP BY department;

-- CORRECT: Use HAVING for aggregates
SELECT department, AVG(salary)
FROM employees
GROUP BY department
HAVING AVG(salary) > 80000;  -- ✓ Correct
```

### Performance Optimization

```sql
-- Less efficient (processes all rows first)
SELECT department, COUNT(*)
FROM employees
GROUP BY department
HAVING department = 'IT';

-- More efficient (filters before grouping)
SELECT department, COUNT(*)
FROM employees
WHERE department = 'IT'  -- Filter early
GROUP BY department;
```

### Best Practice

**Use WHERE to filter rows, HAVING to filter groups:**

```sql
-- Good practice
SELECT 
    department,
    AVG(salary) as avg_salary,
    COUNT(*) as employee_count
FROM employees
WHERE hire_date >= '2020-01-01'     -- Filter rows (WHERE)
  AND status = 'Active'
GROUP BY department
HAVING COUNT(*) > 10                -- Filter groups (HAVING)
   AND AVG(salary) > 75000;
```

---

## 93. Explain INNER JOIN, LEFT JOIN, RIGHT JOIN, and FULL OUTER JOIN with examples

### Sample Tables

```sql
-- Employees table
CREATE TABLE employees (
    employee_id INT PRIMARY KEY,
    name VARCHAR(100),
    department_id INT
);

INSERT INTO employees VALUES
(1, 'Alice', 10),
(2, 'Bob', 20),
(3, 'Charlie', 10),
(4, 'David', NULL),  -- No department
(5, 'Eve', 30);

-- Departments table
CREATE TABLE departments (
    department_id INT PRIMARY KEY,
    department_name VARCHAR(100)
);

INSERT INTO departments VALUES
(10, 'IT'),
(20, 'Sales'),
(40, 'Marketing');  -- No employees
```

### INNER JOIN

**Definition:** Returns only matching rows from both tables.

```sql
SELECT 
    e.name,
    d.department_name
FROM employees e
INNER JOIN departments d ON e.department_id = d.department_id;

-- Result:
-- Alice    | IT
-- Bob      | Sales
-- Charlie  | IT
-- (David not included - NULL department)
-- (Eve not included - department 30 doesn't exist)
-- (Marketing not included - no employees)
```

**Venn Diagram:** Intersection of both circles

### LEFT JOIN (LEFT OUTER JOIN)

**Definition:** Returns all rows from left table, matching rows from right table, NULL for non-matches.

```sql
SELECT 
    e.name,
    d.department_name
FROM employees e
LEFT JOIN departments d ON e.department_id = d.department_id;

-- Result:
-- Alice    | IT
-- Bob      | Sales
-- Charlie  | IT
-- David    | NULL        ← Included (left table)
-- Eve      | NULL        ← Included (left table, no matching dept)
-- (Marketing not included - not in left table)
```

**Venn Diagram:** Entire left circle + intersection

### RIGHT JOIN (RIGHT OUTER JOIN)

**Definition:** Returns all rows from right table, matching rows from left table, NULL for non-matches.

```sql
SELECT 
    e.name,
    d.department_name
FROM employees e
RIGHT JOIN departments d ON e.department_id = d.department_id;

-- Result:
-- Alice    | IT
-- Bob      | Sales
-- Charlie  | IT
-- NULL     | Marketing   ← Included (right table, no employees)
-- (David not included - NULL department)
-- (Eve not included - department 30 doesn't exist in right table)
```

**Venn Diagram:** Intersection + entire right circle

### FULL OUTER JOIN

**Definition:** Returns all rows from both tables, with NULLs for non-matches.

```sql
SELECT 
    e.name,
    d.department_name
FROM employees e
FULL OUTER JOIN departments d ON e.department_id = d.department_id;

-- Result:
-- Alice    | IT
-- Bob      | Sales
-- Charlie  | IT
-- David    | NULL        ← From left table
-- Eve      | NULL        ← From left table
-- NULL     | Marketing   ← From right table
```

**Venn Diagram:** Both circles entirely

### Visual Comparison

```
Tables:
Employees: Alice(10), Bob(20), Charlie(10), David(NULL), Eve(30)
Departments: 10(IT), 20(Sales), 40(Marketing)

INNER JOIN:
Alice-IT, Bob-Sales, Charlie-IT

LEFT JOIN:
Alice-IT, Bob-Sales, Charlie-IT, David-NULL, Eve-NULL

RIGHT JOIN:
Alice-IT, Bob-Sales, Charlie-IT, NULL-Marketing

FULL OUTER JOIN:
Alice-IT, Bob-Sales, Charlie-IT, David-NULL, Eve-NULL, NULL-Marketing
```

### Practical Example: Order System

```sql
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    name VARCHAR(100)
);

CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    amount DECIMAL(10,2)
);

INSERT INTO customers VALUES
(1, 'Alice'),
(2, 'Bob'),
(3, 'Charlie');

INSERT INTO orders VALUES
(101, 1, 100.00),
(102, 1, 150.00),
(103, 2, 200.00),
(104, 4, 75.00);  -- Customer 4 doesn't exist!

-- INNER JOIN: Only customers with orders
SELECT c.name, o.order_id, o.amount
FROM customers c
INNER JOIN orders o ON c.customer_id = o.customer_id;
-- Result: Alice(2 orders), Bob(1 order)

-- LEFT JOIN: All customers, whether they have orders or not
SELECT c.name, o.order_id, o.amount
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id;
-- Result: Alice(2 rows), Bob(1 row), Charlie(1 row with NULL order)

-- RIGHT JOIN: All orders, whether customer exists or not
SELECT c.name, o.order_id, o.amount
FROM customers c
RIGHT JOIN orders o ON c.customer_id = o.customer_id;
-- Result: Alice(2 rows), Bob(1 row), NULL(1 row for order 104)

-- FULL OUTER JOIN: Everything
SELECT c.name, o.order_id, o.amount
FROM customers c
FULL OUTER JOIN orders o ON c.customer_id = o.customer_id;
-- Result: Alice(2 rows), Bob(1 row), Charlie(NULL order), NULL(order 104)
```

### Finding Non-Matches

```sql
-- Customers with no orders (LEFT JOIN + WHERE NULL)
SELECT c.customer_id, c.name
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
WHERE o.order_id IS NULL;
-- Result: Charlie

-- Orders with no customer (RIGHT JOIN + WHERE NULL)
SELECT o.order_id, o.amount
FROM customers c
RIGHT JOIN orders o ON c.customer_id = o.customer_id
WHERE c.customer_id IS NULL;
-- Result: order_id 104

-- Rows that don't match (FULL OUTER JOIN + WHERE NULL)
SELECT 
    c.customer_id,
    c.name,
    o.order_id
FROM customers c
FULL OUTER JOIN orders o ON c.customer_id = o.customer_id
WHERE c.customer_id IS NULL OR o.order_id IS NULL;
-- Result: Charlie (no orders), order 104 (no customer)
```

---

## 94. What is a self-join? Provide an example.

### Definition

**Self-Join:** Joining a table to itself, treating it as two separate tables.

**Use Cases:**
- Hierarchical data (employees and managers)
- Comparing rows within same table
- Finding relationships within data

### Example 1: Employee-Manager Hierarchy

```sql
-- Employees table with manager reference
CREATE TABLE employees (
    employee_id INT PRIMARY KEY,
    name VARCHAR(100),
    manager_id INT,
    salary DECIMAL(10,2)
);

INSERT INTO employees VALUES
(1, 'Alice', NULL, 150000),      -- CEO (no manager)
(2, 'Bob', 1, 120000),           -- Reports to Alice
(3, 'Charlie', 1, 130000),       -- Reports to Alice
(4, 'David', 2, 90000),          -- Reports to Bob
(5, 'Eve', 2, 95000),            -- Reports to Bob
(6, 'Frank', 3, 85000);          -- Reports to Charlie

-- Self-join to get employee with their manager
SELECT 
    e.employee_id,
    e.name as employee_name,
    e.salary as employee_salary,
    m.name as manager_name,
    m.salary as manager_salary
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.employee_id;

-- Result:
-- 1 | Alice   | 150000 | NULL    | NULL      (CEO has no manager)
-- 2 | Bob     | 120000 | Alice   | 150000
-- 3 | Charlie | 130000 | Alice   | 150000
-- 4 | David   | 90000  | Bob     | 120000
-- 5 | Eve     | 95000  | Bob     | 120000
-- 6 | Frank   | 85000  | Charlie | 130000
```

### Example 2: Find Employees in Same Department

```sql
CREATE TABLE employees (
    employee_id INT PRIMARY KEY,
    name VARCHAR(100),
    department VARCHAR(50)
);

INSERT INTO employees VALUES
(1, 'Alice', 'IT'),
(2, 'Bob', 'IT'),
(3, 'Charlie', 'Sales'),
(4, 'David', 'Sales'),
(5, 'Eve', 'HR');

-- Find pairs of employees in same department
SELECT 
    e1.name as employee1,
    e2.name as employee2,
    e1.department
FROM employees e1
INNER JOIN employees e2 
    ON e1.department = e2.department
    AND e1.employee_id < e2.employee_id;  -- Avoid duplicates and self-pairs

-- Result:
-- Alice   | Bob     | IT
-- Charlie | David   | Sales
```

### Example 3: Compare Salaries

```sql
-- Find employees earning more than average in their department
SELECT 
    e1.name,
    e1.salary,
    e1.department,
    AVG(e2.salary) as dept_avg_salary
FROM employees e1
INNER JOIN employees e2 ON e1.department = e2.department
GROUP BY e1.employee_id, e1.name, e1.salary, e1.department
HAVING e1.salary > AVG(e2.salary);
```

### Example 4: Find Consecutive Records

```sql
CREATE TABLE stock_prices (
    date DATE PRIMARY KEY,
    price DECIMAL(10,2)
);

INSERT INTO stock_prices VALUES
('2024-01-01', 100.00),
('2024-01-02', 102.00),
('2024-01-03', 98.00),
('2024-01-04', 105.00),
('2024-01-05', 103.00);

-- Find days where price increased from previous day
SELECT 
    t1.date,
    t1.price as current_price,
    t2.price as previous_price,
    t1.price - t2.price as price_change
FROM stock_prices t1
INNER JOIN stock_prices t2 
    ON t1.date = t2.date + INTERVAL '1 day'
WHERE t1.price > t2.price;

-- Result:
-- 2024-01-02 | 102.00 | 100.00 | 2.00
-- 2024-01-04 | 105.00 | 98.00  | 7.00
```

### Example 5: Organizational Hierarchy (Multiple Levels)

```sql
-- Get full reporting chain
SELECT 
    e.name as employee,
    m1.name as manager,
    m2.name as manager_of_manager,
    m3.name as director
FROM employees e
LEFT JOIN employees m1 ON e.manager_id = m1.employee_id
LEFT JOIN employees m2 ON m1.manager_id = m2.employee_id
LEFT JOIN employees m3 ON m2.manager_id = m3.employee_id;
```

### Recursive CTE for Hierarchies (Modern Approach)

```sql
-- Get all levels of hierarchy recursively
WITH RECURSIVE hierarchy AS (
    -- Base case: employees with no manager (CEO)
    SELECT 
        employee_id,
        name,
        manager_id,
        1 as level,
        name as path
    FROM employees
    WHERE manager_id IS NULL
    
    UNION ALL
    
    -- Recursive case: employees with managers
    SELECT 
        e.employee_id,
        e.name,
        e.manager_id,
        h.level + 1,
        h.path || ' > ' || e.name
    FROM employees e
    INNER JOIN hierarchy h ON e.manager_id = h.employee_id
)
SELECT * FROM hierarchy
ORDER BY level, name;

-- Result shows full org chart with levels:
-- Alice   | Level 1 | Alice
-- Bob     | Level 2 | Alice > Bob
-- Charlie | Level 2 | Alice > Charlie
-- David   | Level 3 | Alice > Bob > David
-- Eve     | Level 3 | Alice > Bob > Eve
-- Frank   | Level 3 | Alice > Charlie > Frank
```

---

## 95-112. Additional SQL Questions (Concise Format)

### 95. What is the difference between DELETE, TRUNCATE, and DROP?

| Aspect | DELETE | TRUNCATE | DROP |
|--------|--------|----------|------|
| **Purpose** | Remove rows | Remove all rows | Remove table |
| **WHERE clause** | ✓ Can filter | ✗ All rows only | N/A |
| **Rollback** | ✓ Can rollback | Limited | ✗ Cannot rollback |
| **Speed** | Slow (row-by-row) | Fast (deallocates pages) | Instant |
| **Triggers** | Fires | No triggers | No triggers |
| **Identity** | Doesn't reset | Resets | N/A |
| **Space** | Doesn't reclaim | Reclaims | Reclaims |

```sql
-- DELETE: Remove specific rows
DELETE FROM employees WHERE department = 'IT';  -- Can rollback

-- TRUNCATE: Remove all rows
TRUNCATE TABLE employees;  -- Fast, resets identity

-- DROP: Remove table entirely
DROP TABLE employees;  -- Table gone
```

### 96. What are window functions? Give examples.

**Definition:** Perform calculations across a set of rows related to the current row.

```sql
-- ROW_NUMBER: Unique sequential number
SELECT 
    name,
    salary,
    ROW_NUMBER() OVER (ORDER BY salary DESC) as row_num
FROM employees;

-- RANK: Ranking with gaps for ties
SELECT 
    name,
    salary,
    RANK() OVER (ORDER BY salary DESC) as rank
FROM employees;

-- DENSE_RANK: Ranking without gaps
SELECT 
    name,
    salary,
    DENSE_RANK() OVER (ORDER BY salary DESC) as dense_rank
FROM employees;

-- PARTITION BY: Window per group
SELECT 
    department,
    name,
    salary,
    AVG(salary) OVER (PARTITION BY department) as dept_avg,
    salary - AVG(salary) OVER (PARTITION BY department) as diff_from_avg
FROM employees;

-- Running total
SELECT 
    date,
    amount,
    SUM(amount) OVER (ORDER BY date) as running_total
FROM sales;

-- Moving average (last 3 rows)
SELECT 
    date,
    price,
    AVG(price) OVER (
        ORDER BY date 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as moving_avg_3day
FROM stock_prices;
```

### 97. Difference between RANK(), DENSE_RANK(), and ROW_NUMBER()

```sql
-- Sample data: salaries 100, 100, 90, 80

-- ROW_NUMBER: Always unique (1, 2, 3, 4)
-- RANK: Gaps for ties (1, 1, 3, 4)
-- DENSE_RANK: No gaps (1, 1, 2, 3)

SELECT 
    name,
    salary,
    ROW_NUMBER() OVER (ORDER BY salary DESC) as row_num,
    RANK() OVER (ORDER BY salary DESC) as rank,
    DENSE_RANK() OVER (ORDER BY salary DESC) as dense_rank
FROM employees;

-- Result:
-- Alice   | 100 | 1 | 1 | 1
-- Bob     | 100 | 2 | 1 | 1  (same rank)
-- Charlie | 90  | 3 | 3 | 2  (RANK skips 2, DENSE_RANK doesn't)
-- David   | 80  | 4 | 4 | 3
```

### 98. What is a CTE (Common Table Expression)?

**Definition:** Temporary named result set that exists within a single query.

```sql
-- Basic CTE
WITH sales_summary AS (
    SELECT 
        department,
        SUM(amount) as total_sales
    FROM sales
    GROUP BY department
)
SELECT * 
FROM sales_summary
WHERE total_sales > 100000;

-- Multiple CTEs
WITH 
high_earners AS (
    SELECT * FROM employees WHERE salary > 100000
),
dept_avg AS (
    SELECT department, AVG(salary) as avg_salary
    FROM employees
    GROUP BY department
)
SELECT h.name, h.salary, d.avg_salary
FROM high_earners h
JOIN dept_avg d ON h.department = d.department;

-- Recursive CTE (hierarchy)
WITH RECURSIVE emp_hierarchy AS (
    SELECT employee_id, name, manager_id, 1 as level
    FROM employees
    WHERE manager_id IS NULL
    
    UNION ALL
    
    SELECT e.employee_id, e.name, e.manager_id, eh.level + 1
    FROM employees e
    JOIN emp_hierarchy eh ON e.manager_id = eh.employee_id
)
SELECT * FROM emp_hierarchy;
```

### 99. What is a subquery? Types of subqueries?

**Types:**

**1. Scalar Subquery (returns single value):**
```sql
SELECT name, salary
FROM employees
WHERE salary > (SELECT AVG(salary) FROM employees);
```

**2. Row Subquery (returns single row):**
```sql
SELECT * FROM employees
WHERE (department, salary) = (
    SELECT department, MAX(salary)
    FROM employees
    GROUP BY department
    LIMIT 1
);
```

**3. Column Subquery (returns single column):**
```sql
SELECT * FROM employees
WHERE department_id IN (
    SELECT department_id FROM departments WHERE location = 'New York'
);
```

**4. Table Subquery (returns multiple rows/columns):**
```sql
SELECT * FROM (
    SELECT department, AVG(salary) as avg_salary
    FROM employees
    GROUP BY department
) dept_salaries
WHERE avg_salary > 80000;
```

**5. Correlated Subquery (references outer query):**
```sql
SELECT e1.name, e1.salary
FROM employees e1
WHERE salary > (
    SELECT AVG(salary)
    FROM employees e2
    WHERE e2.department = e1.department  -- Correlated
);
```

### 100. Difference between IN, EXISTS, and ANY/ALL

```sql
-- IN: Check if value in list
SELECT * FROM employees
WHERE department_id IN (10, 20, 30);

SELECT * FROM employees
WHERE department_id IN (SELECT department_id FROM departments WHERE active = 1);

-- EXISTS: Check if subquery returns any rows (faster for large datasets)
SELECT * FROM employees e
WHERE EXISTS (
    SELECT 1 FROM orders o WHERE o.employee_id = e.employee_id
);

-- NOT EXISTS: Employees with no orders
SELECT * FROM employees e
WHERE NOT EXISTS (
    SELECT 1 FROM orders o WHERE o.employee_id = e.employee_id
);

-- ANY: Compare with any value in list
SELECT * FROM products
WHERE price > ANY (SELECT price FROM products WHERE category = 'Electronics');
-- Same as: price > MIN(prices of Electronics)

-- ALL: Compare with all values in list
SELECT * FROM products
WHERE price > ALL (SELECT price FROM products WHERE category = 'Electronics');
-- Same as: price > MAX(prices of Electronics)
```

**Performance:**
- EXISTS: Often faster (stops at first match)
- IN: Good for small lists
- ANY/ALL: Less common, specific use cases

### 101. What is the difference between CHAR and VARCHAR?

| Aspect | CHAR | VARCHAR |
|--------|------|---------|
| **Storage** | Fixed length | Variable length |
| **Padding** | Padded with spaces | No padding |
| **Performance** | Faster (fixed size) | Slower (variable) |
| **Space** | Can waste space | Efficient |
| **Use Case** | Fixed-length data | Variable-length data |

```sql
-- CHAR(10)
'ABC' → 'ABC       ' (7 spaces added, always 10 bytes)

-- VARCHAR(10)
'ABC' → 'ABC' (3 bytes + length overhead)

-- Examples
state_code CHAR(2)        -- 'NY', 'CA' (always 2 chars)
country_code CHAR(3)      -- 'USA', 'GBR' (always 3 chars)
name VARCHAR(100)         -- Variable length names
email VARCHAR(255)        -- Variable length emails
```

### 102. What are indexes and types of indexes?

**Purpose:** Speed up data retrieval.

**Types:**

**1. Clustered Index:**
- Sorts table data physically
- One per table
- Usually on primary key

```sql
CREATE CLUSTERED INDEX idx_employee_id ON employees(employee_id);
```

**2. Non-Clustered Index:**
- Separate structure with pointers
- Multiple per table

```sql
CREATE NONCLUSTERED INDEX idx_email ON employees(email);
```

**3. Unique Index:**
- Ensures uniqueness

```sql
CREATE UNIQUE INDEX idx_email ON employees(email);
```

**4. Composite Index:**
- Multiple columns

```sql
CREATE INDEX idx_name_dept ON employees(last_name, first_name, department);
```

**5. Covering Index:**
- Includes all query columns

```sql
CREATE INDEX idx_covering ON employees(department) INCLUDE (salary, name);
```

### 103. What is normalization? First 3 normal forms?

**1NF:** Atomic values, no repeating groups
```sql
-- Before: phone = '555-1234, 555-5678'
-- After: Separate table for phone numbers
```

**2NF:** 1NF + No partial dependencies
```sql
-- Before: (order_id, product_id) → product_name
-- After: product_name in separate products table
```

**3NF:** 2NF + No transitive dependencies
```sql
-- Before: employee → department → department_head
-- After: department_head in departments table
```

### 104. What are constraints in SQL?

```sql
-- NOT NULL
name VARCHAR(100) NOT NULL

-- UNIQUE
email VARCHAR(100) UNIQUE

-- PRIMARY KEY (NOT NULL + UNIQUE)
customer_id INT PRIMARY KEY

-- FOREIGN KEY
FOREIGN KEY (customer_id) REFERENCES customers(customer_id)

-- CHECK
age INT CHECK (age >= 18)

-- DEFAULT
status VARCHAR(20) DEFAULT 'Active'

-- Combined example
CREATE TABLE employees (
    employee_id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    age INT CHECK (age BETWEEN 18 AND 65),
    department_id INT,
    status VARCHAR(20) DEFAULT 'Active',
    FOREIGN KEY (department_id) REFERENCES departments(department_id)
);
```

### 105. Difference between primary key and unique key?

| Aspect | Primary Key | Unique Key |
|--------|------------|------------|
| **NULL** | Not allowed | Allowed (one NULL) |
| **Count** | One per table | Multiple per table |
| **Index** | Clustered | Non-clustered |
| **Purpose** | Main identifier | Alternative identifier |

```sql
CREATE TABLE employees (
    employee_id INT PRIMARY KEY,           -- One PK, no NULLs
    email VARCHAR(100) UNIQUE,             -- UK, allows NULL
    ssn VARCHAR(11) UNIQUE,                -- Another UK
    phone VARCHAR(20) UNIQUE               -- Another UK
);
```

### 106. What is the difference between COALESCE and ISNULL?

```sql
-- ISNULL (SQL Server): Returns first non-null (2 parameters only)
SELECT ISNULL(NULL, 'default')  -- 'default'
SELECT ISNULL(phone, 'N/A')

-- COALESCE (Standard SQL): Returns first non-null (multiple parameters)
SELECT COALESCE(NULL, NULL, 'default', 'another')  -- 'default'
SELECT COALESCE(phone, mobile, email, 'No contact')

-- COALESCE is more portable and flexible
```

### 107. What are aggregate functions?

```sql
-- COUNT: Number of rows
SELECT COUNT(*) FROM employees;
SELECT COUNT(DISTINCT department) FROM employees;

-- SUM: Total
SELECT SUM(salary) FROM employees;

-- AVG: Average
SELECT AVG(salary) FROM employees;

-- MIN/MAX: Minimum/Maximum
SELECT MIN(salary), MAX(salary) FROM employees;

-- GROUP_CONCAT/STRING_AGG: Concatenate
SELECT department, STRING_AGG(name, ', ') as employees
FROM employees
GROUP BY department;

-- Example with GROUP BY
SELECT 
    department,
    COUNT(*) as employee_count,
    AVG(salary) as avg_salary,
    MIN(salary) as min_salary,
    MAX(salary) as max_salary,
    SUM(salary) as total_payroll
FROM employees
GROUP BY department;
```

### 108. Explain CASE statement

```sql
-- Simple CASE
SELECT 
    name,
    salary,
    CASE department
        WHEN 'IT' THEN 'Technology'
        WHEN 'HR' THEN 'Human Resources'
        ELSE 'Other'
    END as department_full_name
FROM employees;

-- Searched CASE
SELECT 
    name,
    salary,
    CASE 
        WHEN salary > 100000 THEN 'High'
        WHEN salary > 70000 THEN 'Medium'
        ELSE 'Low'
    END as salary_grade
FROM employees;

-- CASE in WHERE
SELECT * FROM employees
WHERE 
    CASE 
        WHEN department = 'IT' THEN salary > 80000
        WHEN department = 'Sales' THEN salary > 60000
        ELSE salary > 50000
    END;

-- CASE in ORDER BY
SELECT name, salary
FROM employees
ORDER BY 
    CASE 
        WHEN department = 'IT' THEN 1
        WHEN department = 'Sales' THEN 2
        ELSE 3
    END,
    salary DESC;
```

### 109. What is the difference between CROSS JOIN and CARTESIAN PRODUCT?

**They are the same thing!**

```sql
-- CROSS JOIN (explicit)
SELECT *
FROM table1
CROSS JOIN table2;

-- Cartesian Product (implicit)
SELECT *
FROM table1, table2;

-- Result: Every row from table1 × every row from table2
-- If table1 has 10 rows and table2 has 5 rows → 50 rows

-- Example
SELECT 
    colors.name as color,
    sizes.name as size
FROM colors
CROSS JOIN sizes;

-- Result: All color-size combinations
-- Red-Small, Red-Medium, Red-Large
-- Blue-Small, Blue-Medium, Blue-Large
```

### 110. What is a VIEW? Difference between VIEW and TABLE?

**VIEW:** Virtual table based on a query.

```sql
-- Create view
CREATE VIEW high_earners AS
SELECT employee_id, name, salary, department
FROM employees
WHERE salary > 100000;

-- Use like a table
SELECT * FROM high_earners WHERE department = 'IT';

-- Update through view (if simple)
UPDATE high_earners SET salary = salary * 1.1 WHERE employee_id = 123;
```

| Aspect | View | Table |
|--------|------|-------|
| **Storage** | No data (query only) | Stores data |
| **Performance** | Slower (executes query) | Faster (direct access) |
| **Updates** | Limited | Full support |
| **Space** | Minimal | Requires storage |
| **Purpose** | Security, simplification | Data storage |

**Materialized View:** Stores results physically (fast, but needs refresh)

```sql
CREATE MATERIALIZED VIEW sales_summary AS
SELECT department, SUM(amount) as total
FROM sales
GROUP BY department;

REFRESH MATERIALIZED VIEW sales_summary;
```

### 111. Explain ACID properties

**A - Atomicity:** All or nothing
```sql
BEGIN TRANSACTION;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;  -- Both succeed or both fail
```

**C - Consistency:** Data remains valid
```sql
-- Constraints ensure consistency
ALTER TABLE accounts ADD CONSTRAINT chk_balance CHECK (balance >= 0);
```

**I - Isolation:** Transactions don't interfere
```sql
-- Transaction 1 and Transaction 2 run independently
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
```

**D - Durability:** Changes persist
```sql
-- After COMMIT, data survives crashes
COMMIT;  -- Permanent
```

### 112. What are transactions and isolation levels?

**Transaction:**
```sql
BEGIN TRANSACTION;
    UPDATE accounts SET balance = balance - 100 WHERE id = 1;
    UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;  -- Success

-- Or
ROLLBACK;  -- Undo
```

**Isolation Levels:**

```sql
-- READ UNCOMMITTED (Lowest isolation, dirty reads)
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

-- READ COMMITTED (No dirty reads)
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

-- REPEATABLE READ (No dirty reads, no non-repeatable reads)
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;

-- SERIALIZABLE (Highest isolation, no anomalies)
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
```

| Level | Dirty Read | Non-Repeatable Read | Phantom Read |
|-------|-----------|---------------------|--------------|
| READ UNCOMMITTED | ✓ Possible | ✓ Possible | ✓ Possible |
| READ COMMITTED | ✗ | ✓ Possible | ✓ Possible |
| REPEATABLE READ | ✗ | ✗ | ✓ Possible |
| SERIALIZABLE | ✗ | ✗ | ✗ |

**Example:**
```sql
-- Transaction 1
BEGIN TRANSACTION;
UPDATE accounts SET balance = 1000 WHERE id = 1;
-- Not committed yet

-- Transaction 2
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SELECT balance FROM accounts WHERE id = 1;  -- Sees 1000 (dirty read)

SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
SELECT balance FROM accounts WHERE id = 1;  -- Waits for commit
```

---

## Bonus: Common SQL Interview Patterns

### Pattern 1: Running Totals
```sql
SELECT 
    date,
    amount,
    SUM(amount) OVER (ORDER BY date) as running_total
FROM sales;
```

### Pattern 2: Gap Detection
```sql
WITH numbered AS (
    SELECT 
        id,
        ROW_NUMBER() OVER (ORDER BY id) as rn
    FROM sequence_table
)
SELECT id + 1 as gap_start
FROM numbered
WHERE id + 1 NOT IN (SELECT id FROM sequence_table);
```

### Pattern 3: Pivoting Data
```sql
-- Rows to columns
SELECT 
    product,
    SUM(CASE WHEN quarter = 'Q1' THEN sales END) as Q1,
    SUM(CASE WHEN quarter = 'Q2' THEN sales END) as Q2,
    SUM(CASE WHEN quarter = 'Q3' THEN sales END) as Q3,
    SUM(CASE WHEN quarter = 'Q4' THEN sales END) as Q4
FROM sales_data
GROUP BY product;
```

### Pattern 4: Median Calculation
```sql
WITH ordered AS (
    SELECT 
        salary,
        ROW_NUMBER() OVER (ORDER BY salary) as rn,
        COUNT(*) OVER () as total
    FROM employees
)
SELECT AVG(salary) as median
FROM ordered
WHERE rn IN (FLOOR((total + 1) / 2.0), CEIL((total + 1) / 2.0));
```

### Pattern 5: Consecutive Days
```sql
WITH consecutive AS (
    SELECT 
        date,
        date - ROW_NUMBER() OVER (ORDER BY date) * INTERVAL '1 day' as grp
    FROM login_dates
)
SELECT 
    MIN(date) as streak_start,
    MAX(date) as streak_end,
    COUNT(*) as consecutive_days
FROM consecutive
GROUP BY grp
HAVING COUNT(*) >= 3;  -- Streaks of 3+ days
```

---

## Summary

This SQL guide covered **27 comprehensive questions** (86-112):

**Query Writing:**
- Second highest salary
- Find duplicates
- Employees earning more than managers
- Nth highest salary
- Delete duplicates

**Concepts:**
- UNION vs UNION ALL
- WHERE vs HAVING
- JOINs (INNER, LEFT, RIGHT, FULL OUTER)
- Self-joins
- DELETE vs TRUNCATE vs DROP

**Advanced:**
- Window functions (ROW_NUMBER, RANK, DENSE_RANK)
- CTEs (Common Table Expressions)
- Subqueries
- EXISTS vs IN
- CASE statements

**Database Design:**
- CHAR vs VARCHAR
- Indexes
- Normalization
- Constraints
- Primary vs Unique keys

**Theory:**
- VIEWs
- ACID properties
- Transactions
- Isolation levels
- Aggregate functions

Each question includes detailed explanations, SQL examples, comparisons, and best practices.

---

*End of SQL Practical & Theory Guide*

**All interview guides completed!**
