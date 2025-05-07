"""
Data extraction module for the Heart Disease ETL pipeline.
"""

import logging
import pandas as pd
from ucimlrepo import fetch_ucirepo

logger = logging.getLogger(__name__)

class HeartDiseaseExtractor:
    """
    Heart Disease data extractor.
    
    This class is responsible for extracting the UCI Heart Disease dataset
    from the UCI ML Repository.
    """
    
    def __init__(self, dataset_id=45):
        """
        Initialize the extractor.
        
        Args:
            dataset_id (int): UCI ML Repository dataset ID for Heart Disease.
                              Default is 45 for the Heart Disease dataset.
        """
        self.dataset_id = dataset_id
        logger.info(f"Initialized extractor for dataset ID {dataset_id}")
    
    def extract(self) -> pd.DataFrame:
        """
        Extract the Heart Disease dataset from the UCI ML Repository.
        
        Returns:
            pd.DataFrame: Raw data from the UCI ML Repository.
        """
        logger.info(f"Extracting data from UCI ML Repository with dataset ID {self.dataset_id}")
        
        try:
            # Fetch the dataset using the ucimlrepo package
            heart_disease = fetch_ucirepo(id=self.dataset_id)
            
            # Extract features and target
            X = heart_disease.data.features
            y = heart_disease.data.targets
            
            # Combine features and target
            raw_data = pd.concat([X, y], axis=1)
            
            logger.info(f"Successfully extracted {len(raw_data)} records with {len(raw_data.columns)} columns")
            logger.info(f"Columns: {', '.join(raw_data.columns)}")
            
            # Log dataset statistics
            self._log_dataset_stats(heart_disease, raw_data)
            
            return raw_data
            
        except Exception as e:
            logger.error(f"Error extracting data: {str(e)}", exc_info=True)
            raise
    
    def _log_dataset_stats(self, heart_disease, raw_data):
        """
        Log dataset statistics.
        
        Args:
            heart_disease: The dataset object from ucimlrepo.
            raw_data (pd.DataFrame): The raw data.
        """
        try:
            # Log metadata
            logger.info(f"Dataset name: {heart_disease.metadata.name}")
            logger.info(f"Number of instances: {heart_disease.metadata.num_instances}")
            logger.info(f"Number of features: {heart_disease.metadata.num_features}")
            
            # Log missing values
            missing_values = raw_data.isnull().sum()
            if missing_values.sum() > 0:
                logger.info(f"Found {missing_values.sum()} missing values in the dataset")
                for col in missing_values[missing_values > 0].index:
                    logger.info(f"Column '{col}' has {missing_values[col]} missing values")
            else:
                logger.info("No missing values found in the dataset")
                
            # Log data types
            logger.info("Column data types:")
            for col, dtype in raw_data.dtypes.items():
                logger.info(f"  {col}: {dtype}")
                
            # Log basic statistics for numeric columns
            logger.debug("Basic statistics for numeric columns:")
            for col in raw_data.select_dtypes(include=['int64', 'float64']).columns:
                stats = raw_data[col].describe()
                logger.debug(f"  {col}:")
                logger.debug(f"    min: {stats['min']}")
                logger.debug(f"    max: {stats['max']}")
                logger.debug(f"    mean: {stats['mean']}")
                logger.debug(f"    std: {stats['std']}")
        
        except Exception as e:
            logger.warning(f"Error logging dataset statistics: {str(e)}")