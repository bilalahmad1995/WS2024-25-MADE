import pandas as pd
import sqlite3
import kaggle
import os
import logging

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SP500ETL:
    def __init__(self):
        # Kaggle information for S&P 500 dataset
        self.kaggle_dataset = "andrewmvd/sp-500-stocks"
        self.filename = "sp500_companies.csv"

    def download_data(self):
        try:
            # Use Kaggle API to download the specified CSV file
            kaggle.api.dataset_download_file(self.kaggle_dataset, file_name=self.filename, path='.')
            
            if not os.path.isfile(self.filename):
                raise FileNotFoundError(f"Error: {self.filename} was not downloaded successfully.")
                
            data = pd.read_csv(self.filename)
            os.remove(self.filename)  # deleting the CSV file
            logging.info("Data downloaded and loaded into DataFrame.")
            return data
            
        except Exception as err:
            logging.error(f"Download error: {err}")
            raise

    def process_data(self, data):
        try:
            # Convert 'Fulltimeemployees' column to integers
            data['Fulltimeemployees'] = pd.to_numeric(data['Fulltimeemployees'], errors='coerce').fillna(0).astype(int)
            
            # Adjust 'Marketcap' column to represent billions
            data['Marketcap'] = (data['Marketcap'] / 1e9).round(1)
            data.rename(columns={'Marketcap': 'Marketcap_in_Billions'}, inplace=True)

            # Adjust 'Ebitda' column to represent billions
            data['Ebitda'] = (data['Ebitda'] / 1e9).round(1)
            data.rename(columns={'Ebitda': 'Ebitda_in_Billions'}, inplace=True)

            logging.info("Data transformation complete.")
            return data
            
        except Exception as err:
            logging.error(f"Transformation error: {err}")
            raise

    def store_data(self, data, database_name, table_name):
        try:
            with sqlite3.connect(database_name) as conn:
                data.to_sql(table_name, conn, if_exists='replace', index=False)
                logging.info(f"Data saved to table '{table_name}' in database '{database_name}'.")
        except Exception as err:
            logging.error(f"Storage error: {err}")
            raise

    def execute_etl(self, database_name, table_name):
        try:
            raw_data = self.download_data()
            processed_data = self.process_data(raw_data)
            self.store_data(processed_data, database_name, table_name)
            logging.info("ETL process successfully completed.")
            return processed_data, database_name, table_name
        except Exception as err:
            logging.error(f"ETL process error: {err}")

# Run the ETL pipeline
etl_pipeline = SP500ETL()
# Run with specified database and table
final_data, db, tbl = etl_pipeline.execute_etl('../data/sp500_data.db', 'sp500_table')
