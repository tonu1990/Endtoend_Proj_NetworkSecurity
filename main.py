from networksecurity.my_components.data_ingestion import DataIngestion
 
from networksecurity.my_exception.my_exception import NetworkSecurityException
from networksecurity.my_logging.my_logger import logging
from networksecurity.my_entity.config_entity import DataIngestionConfig
from networksecurity.my_entity.config_entity import TrainingPipelineConfig

import sys

if __name__=='__main__':
    try:
        trainingpipelineconfig=TrainingPipelineConfig()
        dataingestionconfig=DataIngestionConfig(trainingpipelineconfig)
        data_ingestion=DataIngestion(dataingestionconfig)
        logging.info("Initiate the data ingestion")
        dataingestionartifact=data_ingestion.initiate_data_ingestion()
        logging.info("Data Initiation Completed")
        print(dataingestionartifact) 
        
        
    except Exception as e:
           raise NetworkSecurityException(e,sys)
