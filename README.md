# Heart Disease ETL Pipeline

This project implements an ETL (Extract, Transform, Load) pipeline for the UCI Heart Disease dataset. The pipeline extracts data from the UCI Machine Learning Repository, transforms it by cleaning and validating it, and loads it into a PostgreSQL database. The system ensures high data quality through comprehensive validation, error handling, and detailed logging of the entire ETL process.

## Design Choices

This ETL pipeline was designed with the following principles in mind:

1. **Modularity**: The code is organized into separate modules for extraction, transformation, and loading, making it easy to maintain and extend.

2. **Robustness**: The pipeline includes comprehensive error handling, validation, and logging to ensure reliability and data quality.

3. **Data Quality Assurance**: Thorough validation and cleaning processes ensure the output data meets quality standards with detailed logs of any inconsistencies.

4. **Scalability**: The loading phase uses chunking to handle large datasets efficiently, allowing the pipeline to scale with dataset size.

5. **Observability**: Human-readable logging provides visibility into each step of the ETL process, recording transformations applied and any data issues encountered.

6. **Reproducibility**: Docker containerization ensures consistent behavior across different environments.

## ETL Process Details

### Extract
- **Data Source**: Heart Disease dataset from the UCI Machine Learning Repository (ID: 45)
- **Method**: Extracts data using the `ucimlrepo` Python package
- **Error Handling**: Gracefully handles connection issues and corrupted data sources
- **Logging**: Records dataset metadata and extraction status

### Transform
- **Data Cleaning**:
  - Standardizes column names to snake_case format
  - Converts date fields to consistent YYYY-MM-DD format
  - Handles missing values through appropriate imputation strategies
  - Validates data types and ensures numeric fields contain only numbers
  - Performs data range validation for critical fields
  - Removes duplicate records based on configurable unique identifiers
  - Normalizes categorical variables for consistency

- **Data Enrichment**:
  - Calculates derived features where applicable
  - Adds metadata columns for tracking data lineage and processing timestamps
  - Provides statistical summaries of the transformation process

### Handling Missing and Inconsistent Data

Our approach to handling missing and inconsistent data follows these strategies:

1. **Missing Values**:
   - For columns with low missing rate (<5%):
     - Numeric columns: Impute with median values
     - Categorical columns: Impute with mode (most frequent value)
   - For columns with high missing rate (>5%):
     - Drop rows where critical data is missing to maintain data integrity

2. **Inconsistent Data**:
   - **Value Range Validation**: Numeric fields are checked against valid ranges (e.g., age: 0-120)
   - **Categorical Data**: Values are validated against pre-defined allowed values
   - **Data Type Enforcement**: Values are coerced to proper data types with explicit error handling
   - **Date Standardization**: Various date formats are converted to YYYY-MM-DD format

3. **Invalid Data Handling**:
   - Values outside valid ranges are replaced with NaN, then imputed
   - Detailed logs record all replacements for transparency
   - Statistics on data quality issues are generated during transformation

4. **Duplicate Records**:
   - Full duplicate records are identified and removed
   - The number of removed duplicates is logged for audit purposes

### Load
- **Database**: PostgreSQL with properly defined schema
- **Approach**: 
  - Creates tables with appropriate data types and constraints
  - Uses batch processing for efficient loading of large datasets
  - Implements transaction handling for data integrity
  - Performs post-load validation to ensure data was loaded correctly

## Technical Requirements

- **Language**: Implemented in Python 3.8+
- **Containerization**: Complete Dockerized setup with Docker Compose
- **Database**: PostgreSQL database for data storage
- **Dependencies**: All required packages listed in requirements.txt
- **End-to-End Automation**: Pipeline runs without user intervention after initial setup

## Features

- **Automated ETL Pipeline**: End-to-end process that requires minimal setup and no user input during execution
- **Human-Readable Logging**: Comprehensive logs that clearly document each step, transformation, and any data issues encountered
- **Data Quality Reports**: Generates summaries of data quality metrics before and after transformation
- **Error Handling**: Robust error handling with appropriate fallback mechanisms
- **Configuration Management**: Externalized configuration for easy customization
- **Performance Optimization**: Efficient processing through vectorized operations and batch loading

## Setup and Usage

### Prerequisites

- Docker and Docker Compose

### Running the Pipeline

1. Clone the repository:
   ```bash
   git clone https://github.com/jahid-hasan-polash/heart_disease_etl
   cd heart_disease_etl
   ```

2. Start the services using Docker Compose:
   ```bash
   docker compose up -d --build
   ```

This will:
- Start a PostgreSQL database
- Build and run the ETL pipeline
- Process the UCI Heart Disease dataset
- Load the data into the database

### Checking the Results

You can connect to the PostgreSQL database to verify the loaded data:

```bash
docker exec -it heart_disease_etl_db_1 psql -U postgres -d heart_disease -c "SELECT COUNT(*) FROM heart_disease;"
```

## Environment Variables

You can customize the pipeline by setting the following environment variables:

- `DB_HOST`: PostgreSQL host (default: `db`)
- `DB_PORT`: PostgreSQL port (default: `5432`)
- `DB_NAME`: PostgreSQL database name (default: `heart_disease`)
- `DB_USER`: PostgreSQL user (default: `postgres`)
- `DB_PASSWORD`: PostgreSQL password (default: `postgres`)
- `LOG_LEVEL`: Logging level (default: `INFO`)
- `DATASET_ID`: UCI ML Repository dataset ID (default: `45` for Heart Disease)
