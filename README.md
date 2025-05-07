# Heart Disease ETL Pipeline

This project implements an ETL (Extract, Transform, Load) pipeline for the UCI Heart Disease dataset. The pipeline extracts data from the UCI Machine Learning Repository, transforms it by cleaning and validating it, and loads it into a PostgreSQL database.

## Project Structure

The project is organized with a modular structure:

```
heart_disease_etl/
│
├── README.md                     # Project documentation
├── requirements.txt              # Python dependencies
├── Dockerfile                    # Docker configuration
├── docker-compose.yml            # Docker Compose configuration
├── main.py                       # Main entry point
│
├── app/                          # Application code
│   ├── __init__.py               # Make app a package
│   ├── config.py                 # Configuration settings
│   │
│   ├── extract/                  # Data extraction module
│   │   ├── __init__.py
│   │   └── extractor.py          # Data extraction logic
│   │
│   ├── transform/                # Data transformation module
│   │   ├── __init__.py
│   │   └── transformer.py        # Data transformation logic
│   │
│   ├── load/                     # Data loading module
│   │   ├── __init__.py
│   │   └── loader.py             # Data loading logic
│   │
│   ├── database/                 # Database operations
│   │   ├── __init__.py
│   │   ├── connection.py         # Database connection management
│   │   └── models.py             # SQLAlchemy ORM models
│   │
│   └── utils/                    # Utility functions
│       ├── __init__.py
│       └── logging_config.py     # Logging configuration
│
└── logs/                         # Log files directory
    └── .gitkeep                  # Keep the directory in git
```

## Design Choices

This ETL pipeline was designed with the following principles in mind:

1. **Modularity**: The code is organized into separate modules for extraction, transformation, and loading, making it easy to maintain and extend.

2. **Robustness**: The pipeline includes comprehensive error handling, validation, and logging to ensure reliability.

3. **Data Quality**: The transformation phase includes steps to handle missing values, standardize formats, remove duplicates, and validate data types.

4. **Scalability**: The loading phase uses chunking to handle large datasets efficiently.

5. **Observability**: Detailed logging provides visibility into the ETL process and helps diagnose issues.

## Features

- **Data Extraction**: Extracts the Heart Disease dataset from the UCI Machine Learning Repository using the `ucimlrepo` package.
- **Data Transformation**:
  - Standardizes column names
  - Converts data types
  - Handles missing values
  - Validates data
  - Removes duplicates
  - Adds metadata for data lineage
- **Data Loading**: Loads the transformed data into a PostgreSQL database with proper data types and constraints.
- **Dockerized Setup**: Includes Docker and Docker Compose configurations for easy deployment.
- **Logging**: Comprehensive logging for monitoring and debugging.

## Setup and Usage

### Prerequisites

- Docker and Docker Compose

### Running the Pipeline

1. Clone the repository:
   ```bash
   git clone [your-repository-url]
   cd heart_disease_etl
   ```

2. Start the services using Docker Compose:
   ```bash
   docker-compose up --build
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

## Development

### Running Locally

To run the pipeline locally without Docker:

1. Install the dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Make sure you have PostgreSQL running and accessible.

3. Run the pipeline:
   ```bash
   python main.py
   ```

### Adding New Features

- To add support for a new dataset, create a new extractor class in the `extract` module.
- To add new transformations, extend the `transformer.py` file.
- To support a different database, create a new loader class in the `load` module.

## License

[MIT License](LICENSE)