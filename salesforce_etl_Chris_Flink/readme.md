# Data Pipeline Documentation

## Overview
This document provides a comprehensive overview of the Salesforce Data Pipeline, including its purpose, flow, data cleaning rules, and assumptions. The pipeline extracts data from various sources, processes and validates it, and loads the cleaned data into an SQLite database for further analysis.

---

## **Pipeline Components**

### 1. **Configuration (`config_json.py` and `salesforce_config.json`)**
- Centralized configuration management.
- Stores file paths, column mappings, and table names.
- Example:
  - Data Source Path: `data/salesforce/`
  - Activities Text Columns: `type`, `outcome`

### 2. **Data Loading and Transformation (`data.py`)**
- Handles data extraction, standardization, validation, and normalization.
- Key Functions:
  - **`source_to_df`**: Loads JSON or CSV files into pandas DataFrames.
  - **`standardize_text_cols`**: Ensures text columns are consistent by stripping whitespace, converting to lowercase, and capitalizing each word.
  - **`normalize_phone_numbers` and `normalize_email_addresses`**: Standardizes contact information.
  - **`remove_duplicate_contacts`**: Deduplicates contacts while preserving relationships.

### 3. **Schema Creation (`sql_schema_creation.py`, `config\schema\init.sql`, and `config\schema\schema_design_decisions.txt`)**
- Generates the SQLite schema based on predefined structure in the init.sql file.
- Ensures relational integrity, indexes, and constraints are in place for performance and integrity.
- Schema Design Decisions are found in the schema_design_decisions.txt file.

### 4. **ETL Execution (`run.py`)**
- The main script to orchestrate the entire pipeline.
- Reads configuration, validates data, and applies cleaning rules before loading data into the database.

### 5. **Logging (`pipeline_logging.py`)**
- Centralized logging setup for pipeline monitoring.
- Logs:
  - Info-level logs for successful operations.
  - Error logs for debugging issues.

---

## **Data Cleaning Rules**

### 1. **Standardization**
- **Text Columns**:
  - Stripped of leading/trailing whitespace.
  - Converted to lowercase.
  - Capitalized each word (e.g., `john doe` → `John Doe`).
- **Date Columns**:
  - Converted to datetime format.
  - Invalid dates coerced to `NaT` and flagged for review.
- **Phone Numbers**:
  - Normalized to the format `+1-555-123-4567`.
  - Non-numeric characters removed, and a default country code (`+1`) added for 10-digit numbers.
- **Email Addresses**:
  - Stripped of whitespace and converted to lowercase.

### 2. **Validation**
- **Column Mapping Validation**:
  - Ensures source files contain expected columns before processing.
  - Ensures SQLite tables contain expected columns before processing.
  - Logs errors if column names do not match.
- **Date Range Validation**:
  - Ensures `created_date` does not exceed `close_date` for opportunities.
- **Referential Integrity**:
  - Validates that `contacts_df.company_id` exists in `companies_df`.
  - Checks that `activities.contact_id` exists in `contacts_df`.

### 3. **Duplicate Removal**
- Duplicates identified based on `email`.
- Retains the most recent record using `last_modified`.
- Updates foreign keys in related tables (`activities` and `opportunities`).

---

## **Key Assumptions**

1. **Data Completeness**:
   - All required fields (e.g., `id`, `email`, `created_date`) are present in source files.
   - Missing optional fields (e.g., `phone`, `title`) are acceptable and handled appropriately.

2. **Date Formats**:
   - Source files follow the standard `YYYY-MM-DD HH:MM:SS.MMMMMM` format for dates.
   - Invalid dates are rare and flagged during cleaning.

3. **Unique Identifiers**:
   - Each table (`companies`, `contacts`, etc.) has a unique primary key (`id`).
   - Duplicates are primarily identified using `email`.

4. **Phone Number Standardization**:
   - Assumes North American phone number format for default normalization (`+1`).

5. **Orphaned Records**:
   - Orphaned `activities` or `opportunities` (due to missing `contact_id` or `company_id`) are rare but flagged during referential integrity checks.

---

## **Pipeline Flow**
1. **Extract**:
   - Reads source files (`CSV` or `JSON`) using `source_to_df`.
   - Validates column mappings before processing.

2. **Pre-Validate**:
   - Validates the Source vs column mapping to ensure the source contains the correct columns
   - Validates the SQLite tables to ensure they contain the correct columns

3. **Transform**:
   - Standardizes and normalizes text and date columns.
   - Removes duplicates and ensures referential integrity.
   - Matches missing `opportunity_id` in `activities` using `contact_id` and timestamp logic.

4. **Load**:
   - Truncates existing SQLite tables.
   - Loads cleaned DataFrames into SQLite using batch inserts.

5. **Post-Validate**:
   - Ensures no orphaned records or invalid date ranges post-load.

---

## **Example Logs**
- **Info Log**:
  ```
  2025-01-23 14:03:25, "SalesforcePipeline", "Schema created successfully."
  2025-01-23 14:05:12, "SalesforcePipeline", "Phone numbers normalized."
  2025-01-23 14:10:45, "SalesforcePipeline", "ETL process completed successfully."
  ```
- **Error Log**:
  ```
  2025-01-23 14:07:32, "SalesforcePipeline", "Cannot normalize email addresses: Invalid email format."
  2025-01-23 14:08:45, "SalesforcePipeline", "Referential integrity violated for activities.contact_id."
  ```

---

## **Sample Queries**
Below are some sample queries to demonstrate data access in the SQLite database:

### 1. Retrieve all activities for a specific contact:
```sql
SELECT a.id, a.type, a.subject, a.timestamp, a.outcome
FROM activities AS a
JOIN contacts AS c ON a.contact_id = c.id
WHERE c.email = 'example@domain.com';
```

### 2. Summarize opportunities by stage:
```sql
SELECT stage, COUNT(*) AS opportunity_count, SUM(value) AS total_value
FROM opportunities
GROUP BY stage;
```

### 3. Fetch companies with their associated contacts:
```sql
SELECT co.name AS company_name, c.first_name, c.last_name, c.email
FROM companies AS co
LEFT JOIN contacts AS c ON co.id = c.company_id
ORDER BY co.name;
```

### 4. Identify contacts with the highest number of activities:
```sql
SELECT c.first_name, c.last_name, COUNT(a.id) AS activity_count
FROM contacts AS c
JOIN activities AS a ON c.id = a.contact_id
GROUP BY c.id
ORDER BY activity_count DESC
LIMIT 10;
```

### 5. Validate opportunities with invalid dates:
```sql
SELECT id, created_date, close_date
FROM opportunities
WHERE created_date > close_date;
```

### 6. Retrieve opportunities for a specific company:
```sql
SELECT o.id, o.name, o.stage, o.value, o.close_date
FROM opportunities AS o
JOIN companies AS co ON o.company_id = co.id
WHERE co.name = 'Example Company';
```

---

## **Conclusion**
This pipeline ensures that Salesforce data is cleaned, validated, and transformed into a reliable format for analysis. 
By following defined rules and assumptions, the pipeline maintains data integrity and provides a scalable foundation for future enhancements.
