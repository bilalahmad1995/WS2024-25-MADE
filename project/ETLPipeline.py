import pandas as pd
import sqlite3
import kaggle
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ETLPipeline:
    def __init__(self):
        # Kaggle dataset and file for S&P 500 data
        self.dataset_kaggle = "andrewmvd/sp-500-stocks"
        self.csv_file_name = "sp500_companies.csv"

    def extract(self):
        try:
            # Download only the specified CSV file directly from Kaggle
            kaggle.api.dataset_download_file(self.dataset_kaggle, file_name=self.csv_file_name, path='.')
            
            # Check if the file was downloaded successfully
            if os.path.exists(self.csv_file_name):
                sp500_df = pd.read_csv(self.csv_file_name)
                
                # Optionally, remove the CSV file after loading it
                os.remove(self.csv_file_name)
                logging.info(f"Successfully extracted 'S&P 500 Stocks' data")
                
            else:
                raise FileNotFoundError(f"File {self.csv_file_name} not found.")
            
            return sp500_df
        
        except Exception as e:
            logging.error(f"An error occurred during extraction: {e}")
            raise

    def transform(self, sp500_df):
        try:
            # Convert 'Fulltimeemployees' column to integer type
            sp500_df['Fulltimeemployees'] = pd.to_numeric(sp500_df['Fulltimeemployees'], errors='coerce').fillna(0).astype(int)

            # Convert 'Marketcap' to billions and rename the column
            sp500_df['Marketcap'] = (sp500_df['Marketcap'] / 1e9).round(1)
            sp500_df.rename(columns={'Marketcap': 'Marketcap_in_BillionDollars'}, inplace=True)

            # Convert 'Ebitda' to billions and rename the column
            sp500_df['Ebitda'] = (sp500_df['Ebitda'] / 1e9).round(1)
            sp500_df.rename(columns={'Ebitda': 'Ebitda_in_BillionDollars'}, inplace=True)

            logging.info("Transformation completed successfully.")
            return sp500_df

        except Exception as e:
            logging.error(f"An error occurred during transformation: {e}")
            raise

    def load(self, dataframe, db_name, table_name):
        try:
            # Connect to SQLite database and store the transformed dataframe in the specified table
            conn = sqlite3.connect(db_name)
            dataframe.to_sql(table_name, conn, if_exists='replace', index=False)
            logging.info(f"Loading completed, data saved to database '{db_name}' in table '{table_name}'.")
        except Exception as e:
            logging.error(f"An error occurred during loading: {e}")
            raise
        finally:
            if 'conn' in locals():
                conn.close()
        return db_name, table_name

    def run(self, db_name, table_name):
        try:
            sp500_df = self.extract()
            transformed_data = self.transform(sp500_df)
            db_name, table_name = self.load(transformed_data, db_name, table_name)
            logging.info("ETL process completed successfully.")
            return transformed_data, db_name, table_name
            
        except Exception as e:
            logging.error(f"An error occurred during the ETL process: {e}")

# Running the ETL pipeline
etl = ETLPipeline()
# Run the pipeline, specifying the database and table name
transformed_data, db_name, table_name = etl.run('../data/sp500_data.db', 'sp500_table')
