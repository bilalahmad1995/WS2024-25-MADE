import pandas as pd
import sqlite3
import kaggle
import os
import logging

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SP500ETL:
    def __init__(self):
        # Kaggle information for S&P 500 datasets
        self.kaggle_dataset_1 = "andrewmvd/sp-500-stocks"
        self.sp500_companies_data = "sp500_companies.csv"
        
        self.kaggle_dataset_2 = "paveljurke/s-and-p-500-gspc-historical-data"
        self.sp500_volume_data = "sap500.csv"

    def download_data(self, dataset, filename):
        try:
            # Use Kaggle API to download the specified CSV file
            kaggle.api.dataset_download_file(dataset, file_name=filename, path='.')
            
            if not os.path.isfile(filename):
                raise FileNotFoundError(f"Error: {filename} was not downloaded successfully.")
                
            data = pd.read_csv(filename)
            os.remove(filename)  # deleting the CSV file
            logging.info(f"Data from {filename} downloaded and loaded into DataFrame.")
            return data
            
        except Exception as err:
            logging.error(f"Download error: {err}")
            raise

    def transform_and_process_data(self, data):
        try:
            # Convert 'Fulltimeemployees' column to integers
            data['Fulltimeemployees'] = pd.to_numeric(data['Fulltimeemployees'], errors='coerce').fillna(0).astype(int)
            
            # Adjust 'Marketcap' column to represent billions
            data['Marketcap'] = (data['Marketcap'] / 1e9).round(1)
            data.rename(columns={'Marketcap': 'Marketcap_in_Billions'}, inplace=True)

            # Adjust 'Ebitda' column to represent billions
            data['Ebitda'] = (data['Ebitda'] / 1e9).round(1)
            data.rename(columns={'Ebitda': 'Ebitda_in_Billions'}, inplace=True)

            data['Weight'] = (data['Weight']).round(2)

            logging.info("Data transformation complete for first dataset.")
            return data
            
        except Exception as err:
            logging.error(f"Transformation error: {err}")
            raise

    def transform_second_dataset(self, data):
        try:
            # Convert 'Date' column to datetime format
            data['Date'] = pd.to_datetime(data['Date'], errors='coerce')
            
            # Drop rows where 'Volume' column is 0
            data = data[data['Volume'] != 0]
            
            # Rename columns for consistency
            data.rename(columns={
                'Date': 'date',
                'Open': 'open_price($)',
                'High': 'high_price($)',
                'Low': 'low_price($)',
                'Close': 'close_price($)',
                'Volume': 'volume'
            }, inplace=True)
            
            # Round 'open_price', 'high_price', 'low_price', and 'close_price' to 2 decimal places
            data['open_price'] = data['open_price'].round(2)
            data['high_price'] = data['high_price'].round(2)
            data['low_price'] = data['low_price'].round(2)
            data['close_price'] = data['close_price'].round(2)

            logging.info("Data transformation complete for second dataset.")
            return data
            
        except Exception as err:
            logging.error(f"Transformation error for second dataset: {err}")
            raise

    def store_data(self, data, database_name, table_name):
        try:
            with sqlite3.connect(database_name) as conn:
                data.to_sql(table_name, conn, if_exists='replace', index=False)
                logging.info(f"Data saved to table '{table_name}' in database '{database_name}'.")
        except Exception as err:
            logging.error(f"Storage error: {err}")
            raise

    def execute_etl(self, database_name):
        try:
            # Process first dataset and store in `sp500_companies`
            raw_data_1 = self.download_data(self.kaggle_dataset_1, self.sp500_companies_data)
            processed_data_1 = self.transform_and_process_data(raw_data_1)
            self.store_data(processed_data_1, database_name, 'sp500_companies')

            # Process second dataset and store in `sp500_stocksprice_and_volume`
            raw_data_2 = self.download_data(self.kaggle_dataset_2, self.sp500_volume_data)
            processed_data_2 = self.transform_second_dataset(raw_data_2)
            self.store_data(processed_data_2, database_name, 'sp500_stocksprice_and_volume')
            
            logging.info("ETL process successfully completed for both datasets.")
            return processed_data_1, processed_data_2, database_name, 'sp500_companies', 'sp500_stocksprice_and_volume'
        except Exception as err:
            logging.error(f"ETL process error: {err}")

# Run the ETL pipeline
etl_pipeline = SP500ETL()
# Run with specified database and tables
final_data_1, final_data_2, db, tbl1, tbl2 = etl_pipeline.execute_etl('../data/sp500_data.db')