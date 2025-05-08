"""
Data transformation module for the Heart Disease ETL pipeline.
"""

import logging
import datetime
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

class HeartDiseaseTransformer:
    """
    Heart Disease data transformer.
    
    This class is responsible for transforming the raw Heart Disease 
    dataset by cleaning, standardizing features, and validating data.
    """
    
    def __init__(self, raw_data: pd.DataFrame):
        """
        Initialize the transformer.
        
        Args:
            raw_data (pd.DataFrame): Raw data to transform.
        """
        self.raw_data = raw_data
        logger.info(f"Initialized transformer with {len(raw_data)} records")
    
    def transform(self) -> pd.DataFrame:
        """
        Transform the raw data.
        
        This method performs the following transformations:
        1. Standardize column names
        2. Convert data types
        3. Handle missing values
        4. Standardize date formats
        5. Validate data
        6. Remove duplicates
        7. Add metadata columns
        
        Returns:
            pd.DataFrame: Transformed data.
        """
        logger.info("Starting data transformation")
        
        try:
            # Create a copy of the raw data to avoid modifying the original
            df = self.raw_data.copy()
            
            # Apply transformations in sequence
            df = self._standardize_column_names(df)
            df = self._convert_data_types(df)
            df = self._handle_missing_values(df)
            df = self._standardize_date_formats(df)
            df = self._validate_data(df)
            df = self._remove_duplicates(df)
            df = self._add_metadata(df)
            
            logger.info(f"Transformation completed. Result has {len(df)} records")
            
            return df
            
        except Exception as e:
            logger.error(f"Error transforming data: {str(e)}", exc_info=True)
            raise
    
    def _standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize column names to lowercase with underscores.
        
        Args:
            df (pd.DataFrame): DataFrame to process.
            
        Returns:
            pd.DataFrame: DataFrame with standardized column names.
        """
        logger.info("Standardizing column names")
        
        # Convert column names to lowercase and replace spaces with underscores
        df.columns = [col.lower().replace(' ', '_') for col in df.columns]
        
        # Rename 'num' column to 'target' if it exists
        if 'num' in df.columns:
            df.rename(columns={'num': 'target'}, inplace=True)
            logger.info("Renamed 'num' column to 'target'")
        
        logger.info(f"Standardized column names: {', '.join(df.columns)}")
        
        return df
    
    def _convert_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert columns to appropriate data types.
        
        Args:
            df (pd.DataFrame): DataFrame to process.
            
        Returns:
            pd.DataFrame: DataFrame with converted data types.
        """
        logger.info("Converting data types")
        
        # Boolean columns
        boolean_cols = ['sex', 'fbs', 'exang']
        for col in boolean_cols:
            if col in df.columns:
                df[col] = df[col].astype(bool)
                logger.info(f"Converted '{col}' to boolean")
        
        # Integer columns
        integer_cols = ['age', 'cp', 'trestbps', 'chol', 'restecg', 'thalach', 'slope', 'ca', 'thal']
        for col in integer_cols:
            if col in df.columns:
                # Convert to float first, then to integer to avoid errors with NaN values
                df[col] = pd.to_numeric(df[col], errors='coerce')
                logger.info(f"Converted '{col}' to numeric")
        
        # Float columns
        float_cols = ['oldpeak']
        for col in float_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                logger.info(f"Converted '{col}' to float")
        
        # Convert target to integer (0-4) and create binary has_disease column
        if 'target' in df.columns:
            df['target'] = pd.to_numeric(df['target'], errors='coerce').astype('Int64')
            # Add binary column indicating presence (1) or absence (0) of heart disease
            df['has_disease'] = (df['target'] > 0).astype(int)
            logger.info("Converted 'target' to integer (0-4) and added 'has_disease' binary column")
        
        return df
    
    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle missing values in the dataset.
        
        Args:
            df (pd.DataFrame): DataFrame to process.
            
        Returns:
            pd.DataFrame: DataFrame with handled missing values.
        """
        logger.info("Handling missing values")
        
        # Check for missing values
        missing_values = df.isnull().sum()
        total_missing = missing_values.sum()
        
        if total_missing > 0:
            logger.info(f"Found {total_missing} missing values across {sum(missing_values > 0)} columns")
            
            # Handle each column with missing values
            for col in df.columns[missing_values > 0]:
                missing_count = missing_values[col]
                missing_percent = (missing_count / len(df)) * 100
                
                logger.info(f"Column '{col}' has {missing_count} missing values ({missing_percent:.2f}%)")
                
                # Strategy based on percentage of missing values
                if missing_percent < 5:
                    # For columns with less than 5% missing values, impute with median (numeric) or mode (categorical)
                    if df[col].dtype in [np.int64, np.float64]:
                        # For numeric columns, impute with median
                        median_value = df[col].median()
                        df[col].fillna(median_value, inplace=True)
                        logger.info(f"Imputed missing values in '{col}' with median: {median_value}")
                    else:
                        # For categorical columns, impute with mode
                        mode_value = df[col].mode()[0]
                        df[col].fillna(mode_value, inplace=True)
                        logger.info(f"Imputed missing values in '{col}' with mode: {mode_value}")
                else:
                    # For columns with more than 5% missing values, drop the rows
                    logger.warning(f"Column '{col}' has {missing_percent:.2f}% missing values, dropping affected rows")
                    df.dropna(subset=[col], inplace=True)
                    logger.info(f"Dropped {missing_count} rows with missing values in '{col}'")
        else:
            logger.info("No missing values found in the dataset")
        
        return df
    
    def _standardize_date_formats(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize date formats to YYYY-MM-DD.
        
        Args:
            df (pd.DataFrame): DataFrame to process.
            
        Returns:
            pd.DataFrame: DataFrame with standardized date formats.
        """
        logger.info("Standardizing date formats")
        
        # List of potential date columns in the dataset
        date_columns = []
        
        # Check if any of the columns might contain dates (based on column name)
        potential_date_cols = [col for col in df.columns if any(date_term in col.lower() 
                                                              for date_term in ['date', 'day', 'time', 'dt'])]
        
        for col in potential_date_cols:
            # Try to convert to datetime
            try:
                # Check if the column contains string data that could be dates
                if df[col].dtype == 'object':
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    non_null_count = df[col].notnull().sum()
                    
                    # If most values were successfully converted, assume it's a date column
                    if non_null_count > 0.5 * len(df):
                        # Convert to standard YYYY-MM-DD format
                        df[col] = df[col].dt.strftime('%Y-%m-%d')
                        date_columns.append(col)
                        logger.info(f"Standardized date format in column '{col}' to YYYY-MM-DD")
            except Exception as e:
                logger.warning(f"Failed to convert column '{col}' to date format: {str(e)}")
        
        if not date_columns:
            logger.info("No date columns identified in the dataset")
        
        return df
    
    def _validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate data and handle invalid values.
        
        Args:
            df (pd.DataFrame): DataFrame to process.
            
        Returns:
            pd.DataFrame: Validated DataFrame.
        """
        logger.info("Validating data")
        
        # Track number of validation issues found
        validation_issues = 0
        
        # Define valid ranges for numeric columns
        valid_ranges = {
            'age': (0, 120),
            'trestbps': (0, 300),  # Resting blood pressure (mm Hg)
            'chol': (0, 600),      # Serum cholesterol (mg/dl)
            'thalach': (0, 250),   # Maximum heart rate
            'oldpeak': (0, 10),    # ST depression
            'ca': (0, 3),          # Number of major vessels
            'slope': (1, 3),       # Slope of ST segment
            'thal': (3, 7),        # Thalassemia types
            'target': (0, 4)       # Heart disease severity (0-4)
        }
        
        # Check and fix out-of-range values
        for col, (min_val, max_val) in valid_ranges.items():
            if col in df.columns:
                # Identify out-of-range values
                out_of_range = ((df[col] < min_val) | (df[col] > max_val)) & df[col].notnull()
                out_of_range_count = out_of_range.sum()
                validation_issues += out_of_range_count
                
                if out_of_range_count > 0:
                    logger.warning(f"Found {out_of_range_count} out-of-range values in '{col}'")
                    
                    # Replace out-of-range values with NaN
                    df.loc[out_of_range, col] = np.nan
                    logger.info(f"Replaced {out_of_range_count} out-of-range values in '{col}' with NaN")
                    
                    # Impute with median
                    median_value = df[col].median()
                    df[col].fillna(median_value, inplace=True)
                    logger.info(f"Imputed NaN values in '{col}' with median: {median_value}")
        
        # Validate categorical columns
        categorical_validations = {
            'cp': [1, 2, 3, 4],       # Chest pain type
            'restecg': [0, 1, 2],     # Resting ECG
            'target': [0, 1, 2, 3, 4], # Target (0 = no disease, 1-4 = disease)
            'has_disease': [0, 1]     # Binary target (0 = no disease, 1 = disease)
        }
        
        for col, valid_values in categorical_validations.items():
            if col in df.columns:
                # Identify invalid values
                invalid = ~df[col].isin(valid_values) & df[col].notnull()
                invalid_count = invalid.sum()
                validation_issues += invalid_count
                
                if invalid_count > 0:
                    logger.warning(f"Found {invalid_count} invalid values in '{col}'")
                    
                    # Replace invalid values with NaN
                    df.loc[invalid, col] = np.nan
                    logger.info(f"Replaced {invalid_count} invalid values in '{col}' with NaN")
                    
                    # Impute with mode
                    mode_value = df[col].mode()[0]
                    df[col].fillna(mode_value, inplace=True)
                    logger.info(f"Imputed NaN values in '{col}' with mode: {mode_value}")
        
        logger.info(f"Data validation complete: found and fixed {validation_issues} issues")
        return df
    
    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicate records from the dataset.
        
        Args:
            df (pd.DataFrame): DataFrame to process.
            
        Returns:
            pd.DataFrame: DataFrame with duplicates removed.
        """
        logger.info("Checking for duplicate records")
        
        # Count duplicates
        duplicate_count = df.duplicated().sum()
        
        if duplicate_count > 0:
            logger.info(f"Found {duplicate_count} duplicate records")
            
            # Remove duplicates
            df = df.drop_duplicates().reset_index(drop=True)
            logger.info(f"Removed {duplicate_count} duplicate records")
        else:
            logger.info("No duplicate records found")
        
        return df
    
    def _add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add metadata columns for data lineage.
        
        Args:
            df (pd.DataFrame): DataFrame to process.
            
        Returns:
            pd.DataFrame: DataFrame with added metadata.
        """
        logger.info("Adding metadata columns")
        
        # Add source column
        df['source'] = 'uci_ml_repo'
        
        # Add processing timestamp
        df['processed_at'] = datetime.datetime.now()
        
        logger.info("Added metadata columns: 'source', 'processed_at'")
        
        return df