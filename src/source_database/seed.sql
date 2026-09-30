-- Stand-in for the transcript's SQL Server: three source tables with Change Data Feed on.
-- Run by the job seed_source_database, which passes :catalog and :schema.
-- Safe to re-run: tables are created only if missing and filled only while empty.
-- Never DROP or CREATE OR REPLACE these tables: that breaks the change feed read by cdc_ingestion.

USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:schema);

CREATE TABLE IF NOT EXISTS customer (
  customer_id   STRING NOT NULL,
  first_name    STRING,
  last_name     STRING,
  date_of_birth DATE,
  email         STRING,
  zip_code      STRING
)
COMMENT 'Source customers (stand-in for SQL Server)'
TBLPROPERTIES (delta.enableChangeDataFeed = true);

CREATE TABLE IF NOT EXISTS policy (
  policy_no      STRING NOT NULL,
  customer_id    STRING,
  chassis_number STRING,
  make           STRING,
  model          STRING,
  model_year     INT,
  coverage       STRING,
  premium        DECIMAL(10, 2),
  deductible     INT,
  start_date     DATE,
  end_date       DATE
)
COMMENT 'Source policies (stand-in for SQL Server)'
TBLPROPERTIES (delta.enableChangeDataFeed = true);

CREATE TABLE IF NOT EXISTS claim (
  claim_no          STRING NOT NULL,
  policy_no         STRING,
  incident_date     DATE,
  incident_type     STRING,
  incident_severity STRING,
  claim_amount      DECIMAL(12, 2),
  updated_at        TIMESTAMP
)
COMMENT 'Source claims (stand-in for SQL Server)'
TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- 7,000 customers.
INSERT INTO customer
SELECT
  format_string('C%06d', id) AS customer_id,
  first_name,
  last_name,
  date_add(DATE'1950-01-01', CAST(rand(1) * 18250 AS INT)) AS date_of_birth,
  lower(concat(first_name, '.', last_name, id, '@example.com')) AS email,
  element_at(array('77002', '77003', '77004', '77006', '77007', '77008', '77019', '77024', '77056', '77098'),
             CAST((id * 7) % 10 AS INT) + 1) AS zip_code
FROM (
  SELECT
    id,
    element_at(array('James', 'Maria', 'Robert', 'Linda', 'Michael', 'Aisha', 'Wei', 'Carlos', 'Priya', 'David'),
               CAST(id % 10 AS INT) + 1) AS first_name,
    element_at(array('Smith', 'Lopez', 'Nguyen', 'Johnson', 'Garcia', 'Patel', 'Kim', 'Brown', 'Davis', 'Martinez'),
               CAST((id DIV 10) % 10 AS INT) + 1) AS last_name
  FROM range(1, 7001)
)
WHERE NOT EXISTS (SELECT 1 FROM customer);

-- 12,000 policies; every customer gets at least one; POL0000001-10 cover the part 1 telematics cars CHS000001-10.
INSERT INTO policy
SELECT
  format_string('POL%07d', id) AS policy_no,
  format_string('C%06d', (id - 1) % 7000 + 1) AS customer_id,
  format_string('CHS%06d', id) AS chassis_number,
  element_at(array('Toyota', 'Honda', 'Ford', 'Tesla', 'BMW', 'Hyundai'), CAST(id % 6 AS INT) + 1) AS make,
  element_at(array('Camry', 'Civic', 'F-150', 'Model 3', 'X5', 'Elantra'), CAST(id % 6 AS INT) + 1) AS model,
  CAST(2015 + id % 11 AS INT) AS model_year,
  element_at(array('COMPREHENSIVE', 'COLLISION', 'LIABILITY'), CAST(id % 3 AS INT) + 1) AS coverage,
  CAST(round(600 + rand(2) * 1400, 2) AS DECIMAL(10, 2)) AS premium,
  element_at(array(250, 500, 1000), CAST((id DIV 3) % 3 AS INT) + 1) AS deductible,
  start_date,
  add_months(start_date, 12) AS end_date
FROM (SELECT id, date_add(DATE'2026-01-01', -CAST(id % 365 AS INT)) AS start_date FROM range(1, 12001))
WHERE NOT EXISTS (SELECT 1 FROM policy);

-- 13,000 claims spread over the policies.
INSERT INTO claim
SELECT
  format_string('CLM%08d', id) AS claim_no,
  format_string('POL%07d', (id * 7919) % 12000 + 1) AS policy_no,
  date_add(DATE'2026-01-01', CAST(rand(3) * 270 AS INT)) AS incident_date,
  element_at(array('COLLISION', 'THEFT', 'WEATHER', 'VANDALISM', 'GLASS'), CAST(id % 5 AS INT) + 1) AS incident_type,
  element_at(array('Trivial Damage', 'Minor Damage', 'Major Damage', 'Total Loss'), CAST(id % 4 AS INT) + 1) AS incident_severity,
  CAST(round(100 + rand(4) * 24900, 2) AS DECIMAL(12, 2)) AS claim_amount,
  current_timestamp() AS updated_at
FROM range(1, 13001)
WHERE NOT EXISTS (SELECT 1 FROM claim);
