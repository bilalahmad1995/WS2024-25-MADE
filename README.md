# S&P 500 Companies and Index Analysis and ETL Pipeline

## Overview

This project implements an ETL (Extract, Transform, Load) pipeline for analyzing S&P 500 company data and historical index performance. The goal is to equip investors and financial strategists with data-driven insights for investment decisions, portfolio management, and risk assessment in the U.S. stock market.

## Key Features

- **ETL Pipeline**:
  - Extracts data directly from Kaggle using the Kaggle API.
  - Transforms the data to ensure consistency, readability, and usability.
  - Loads the processed data into a lightweight SQLite database for analysis.
- **Data Analysis**:
  - Provides insights into market capitalization, revenue growth, EBITDA, and workforce distribution across sectors.
  - Examines historical market trends, trading volumes, and volatility of the S&P 500 index.

## Technologies Used

- **Programming Language**: Python
- **Libraries**:
  - Pandas (data manipulation and preprocessing)
  - SQLite (data storage)
  - Kaggle API (data extraction)
  - Logging (pipeline monitoring and debugging)
- **Tools**:
  - VS Code (development environment)
  - Jupyter Notebook (exploratory data analysis)

## ETL Pipeline Process

### 1. Extraction

- Data is retrieved directly from Kaggle using the Kaggle API.
- Two datasets are used:
  1. **S&P 500 Companies Data**: Contains company-level information such as sector, market capitalization, EBITDA, and employee count.
  2. **S&P 500 Index Data**: Historical data including daily open, high, low, close, and trading volumes.
- Both datasets are public domain (CC0 1.0 license). [License: CC0 1.0 Universal (Public Domain Dedication)](https://creativecommons.org/publicdomain/zero/1.0/deed.en)

### 2. Transformation

- Data cleaning and preprocessing steps include:
  - Converting column data types.
  - Renaming columns for clarity.
  - Handling missing values and scaling metrics (e.g., market cap and EBITDA to billions).
  - Removing invalid or unnecessary data (e.g., rows with zero trading volume).
- Ensures consistency and reliability for downstream analysis.

### 3. Loading

- The transformed data is stored in an SQLite database.
- Two tables are created:
  - `sp500_companies`: Contains details of S&P 500 companies.
  - `sp500_stocksprice_and_volume`: Contains historical index data.

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/bilalahmad1995/WS2024-25-MADE.git
cd project
```
