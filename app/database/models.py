"""
SQLAlchemy ORM models for the Heart Disease ETL pipeline.
"""

import datetime
from sqlalchemy import Column, Integer, Float, String, Boolean, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class HeartDisease(Base):
    """
    SQLAlchemy model for the Heart Disease dataset.
    
    This model represents a record in the heart_disease table with all
    the relevant fields from the UCI dataset.
    """
    __tablename__ = 'heart_disease'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Original UCI dataset fields
    age = Column(Integer, nullable=False, comment='Age in years')
    sex = Column(Boolean, nullable=False, comment='Sex (True = male; False = female)')
    cp = Column(Integer, nullable=True, comment='Chest pain type (1-4)')
    trestbps = Column(Integer, nullable=True, comment='Resting blood pressure in mm Hg')
    chol = Column(Integer, nullable=True, comment='Serum cholesterol in mg/dl')
    fbs = Column(Boolean, nullable=True, comment='Fasting blood sugar > 120 mg/dl (True/False)')
    restecg = Column(Integer, nullable=True, comment='Resting electrocardiographic results (0-2)')
    thalach = Column(Integer, nullable=True, comment='Maximum heart rate achieved')
    exang = Column(Boolean, nullable=True, comment='Exercise induced angina (True/False)')
    oldpeak = Column(Float, nullable=True, comment='ST depression induced by exercise relative to rest')
    slope = Column(Integer, nullable=True, comment='Slope of the peak exercise ST segment (1-3)')
    ca = Column(Integer, nullable=True, comment='Number of major vessels colored by fluoroscopy (0-3)')
    thal = Column(Integer, nullable=True, comment='Thalassemia (3 = normal; 6 = fixed defect; 7 = reversible defect)')
    target = Column(Integer, nullable=False, comment='Diagnosis of heart disease (0 = no, 1 = yes)')
    
    # Metadata for data lineage
    source = Column(String, nullable=False, comment='Source of the data')
    processed_at = Column(DateTime, nullable=False, default=datetime.datetime.now, 
                         comment='When the record was processed')
    
    def __repr__(self):
        """String representation of the model."""
        return f"<HeartDisease(id={self.id}, age={self.age}, target={self.target})>"