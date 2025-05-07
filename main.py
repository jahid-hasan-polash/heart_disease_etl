"""
Heart Disease ETL Pipeline

Main entry point for the ETL pipeline that processes the UCI Heart Disease dataset.
"""

import sys
import logging
from app.utils.logging_config import setup_logging
from app.extract.extractor import HeartDiseaseExtractor
from app.transform.transformer import HeartDiseaseTransformer
from app.load.loader import HeartDiseaseLoader
from app.config import Config

def main():
    """
    Run the complete ETL pipeline.
    """
    # Setup logging
    logger = setup_logging()
    logger.info("Starting Heart Disease ETL pipeline")
    
    try:
        # Initialize configuration
        config = Config()
        logger.info(f"Using database URI: {config.get_db_uri()}")
        
        # Extract
        logger.info("Starting extraction phase")
        extractor = HeartDiseaseExtractor()
        raw_data = extractor.extract()
        logger.info(f"Extracted {len(raw_data)} records")
        
        # Transform
        logger.info("Starting transformation phase")
        transformer = HeartDiseaseTransformer(raw_data)
        transformed_data = transformer.transform()
        logger.info(f"Transformed data has {len(transformed_data)} records")
        
        # Load
        logger.info("Starting loading phase")
        loader = HeartDiseaseLoader(config.get_db_uri())
        records_loaded = loader.load(transformed_data)
        logger.info(f"Loaded {records_loaded} records into the database")
        
        logger.info("ETL pipeline completed successfully")
        return 0
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())