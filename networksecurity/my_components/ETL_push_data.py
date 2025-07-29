# -------------------------------
# ETL Script: CSV → MongoDB
# -------------------------------

import os
import sys
import json
import certifi

import pandas as pd
import numpy as np
import pymongo

from dotenv import load_dotenv

# Custom exception and logger (defined elsewhere in your project)
from networksecurity.my_exception.my_exception import NetworkSecurityException
from networksecurity.my_logging.my_logger import logging

# Load environment variables from .env file
load_dotenv()

# Get MongoDB connection string and SSL certificate path
MONGO_DB_URL = os.getenv("MONGO_DB_URL")
ca = certifi.where()


class NetworkDataExtract():
    """
    Class to extract data from a CSV and load it into MongoDB.
    """

    def __init__(self):
        try:
            pass  # No initialization logic needed for now
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def csv_to_json_convertor(self, file_path):
        """
        Extract + Transform:
        Reads a CSV file and converts each row into a JSON-style dictionary (MongoDB document).
        Returns a list of such documents.
        """
        try:
            # Read the CSV file as a DataFrame
            data = pd.read_csv(file_path)

            # Reset index to remove any old indexing
            data.reset_index(drop=True, inplace=True)

            # Convert DataFrame to JSON and then parse into list of dicts
            records = list(json.loads(data.T.to_json()).values())

            return records
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def insert_data_mongodb(self, records, database, collection):
        """
        Load:
        Inserts a list of records (documents) into a MongoDB collection.

        Parameters:
        - records: List of Python dictionaries (documents) to insert
        - database: Name of the MongoDB database (like a folder)
        - collection: Name of the collection (like a table inside the database)

        MongoDB Structure:
        - MongoDB Server
          └── Database (e.g., 'TONU_db')
               └── Collection (e.g., 'NetworkData')
                    └── Document (e.g., one row from the CSV)
        """
        try:
            # Save parameters
            self.database = database
            self.collection = collection
            self.records = records

            # Connect to MongoDB with SSL/TLS verification
            self.mongo_client = pymongo.MongoClient(MONGO_DB_URL, tlsCAFile=ca)

            # Get the specified database from MongoDB server
            self.database = self.mongo_client[self.database]

            # Get the specified collection from the database
            self.collection = self.database[self.collection]

            # Insert all documents at once
            self.collection.insert_many(self.records)

            # Return number of records inserted
            return len(self.records)
        except Exception as e:
            raise NetworkSecurityException(e, sys)


# ------------------------------------------------------
# Main Execution Block: Only runs if this is the main script
# ------------------------------------------------------
if __name__ == '__main__':
    # Path to the CSV file (your dataset)
    FILE_PATH = "Network_Data\\phisingData.csv"

    # MongoDB target database and collection names
    DATABASE = "TONU_db"            # MongoDB database name (like a project folder)
    Collection = "NetworkData_2"      # Collection name (like a SQL table)

    # Create an instance of the ETL class
    networkobj = NetworkDataExtract()

    # Step 1: Extract & Transform
    records = networkobj.csv_to_json_convertor(file_path=FILE_PATH)
    print("Sample Records Extracted:")
    print(records[:2])  # Show first 2 records

    # Step 2: Load
    no_of_records = networkobj.insert_data_mongodb(records, DATABASE, Collection)
    print(f"\n Successfully inserted {no_of_records} records into MongoDB.")
