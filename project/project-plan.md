# Project Plan

## Title

<!-- Give your project a short title. -->

S&P 500 Companies Data and historical S&P 500 index data

## Main Question

<!-- Think about one main question you want to answer based on the data. -->

1. What sectors demonstrate the highest market capitalization and revenue growth in the S&P 500, and how do they compare in terms of EBITDA and employee count?

## Description

This project aims to provide a comprehensive analysis of the S&P 500, leveraging both company-level financial metrics and historical market activity to uncover key trends and insights. The analysis combines two complementary perspectives: sectoral performance and historical market behavior.

On the company level, the project seeks to identify sectors with the highest market capitalization and revenue growth, offering insights into high-growth areas within the U.S. stock market. Attributes such as sector, industry, current price, market capitalization, EBITDA, revenue growth, and employee count will be analyzed to present a holistic view of financial health and workforce distribution across major sectors. By calculating sector-wise averages and utilizing visualizations, the analysis highlights variations in growth potential and profitability among sectors, helping investors and strategists focus on high-performing areas.

On the market level, the project investigates historical S&P 500 index data, including attributes such as date, opening price, highest price, lowest price, closing price, and trading volume. Through data preprocessing, transformations, and exploratory data analysis (EDA), the study uncovers patterns in daily market activity, fluctuations in closing prices, trends in trading volume, and market volatility reflected in the high-low price spread. Monthly averages and correlations between trading volume and closing prices are explored to provide a long-term perspective on market dynamics.

This dual analysis benefits investors, market analysts, and financial strategists by delivering data-driven insights into both sectoral growth potential and historical market trends. Understanding how sectors perform financially and how the market has evolved over time enables stakeholders to make informed decisions regarding investments, portfolio management, and risk assessment. By combining these perspectives, the project aims to provide a robust foundation for strategic decision-making in the U.S. stock market.

### Datasource1:

- Metadata URL: https://www.kaggle.com/datasets/andrewmvd/sp-500-stocks/data
- Data URL: https://www.kaggle.com/datasets/andrewmvd/sp-500-stocks?select=sp500_companies.csv
- Data Type: CSV

### Datasource2:

- Metadata URL: https://www.kaggle.com/datasets/paveljurke/s-and-p-500-gspc-historical-data
- Data URL: https://www.kaggle.com/datasets/paveljurke/s-and-p-500-gspc-historical-data?select=sap500.csv
- Data Type: CSV

## License

CC0: Public Domain: https://creativecommons.org/publicdomain/zero/1.0/

## Work Packages:

1. Identify Relevant Datasets. [#1][i1]
2. Establish Initial Project Plan. [#2][i2]
3. Set Up Data Extraction and Kaggle API. [#3][i3]
4. Perform Data Transformation. [#4][i4]
5. Load Data into Target Destination. [#5][i5]
6. Develop `pipeline.sh` Script to Test ETL_Pipeline. [#6][i6]
7. Configure CI Pipeline for Project Automation. [#7][i7]
8. Prepare Final Project Documentation. [#8][i8]

[i1]: https://github.com/bilalahmad1995/WS2024-25-MADE
[i2]: https://github.com/bilalahmad1995/WS2024-25-MADE
[i3]: https://github.com/bilalahmad1995/WS2024-25-MADE
[i4]: https://github.com/bilalahmad1995/WS2024-25-MADE
[i5]: https://github.com/bilalahmad1995/WS2024-25-MADE
[i6]: https://github.com/bilalahmad1995/WS2024-25-MADE/blob/main/project/pipeline.sh
[i7]: https://github.com/bilalahmad1995/WS2024-25-MADE/actions/runs/12297559557
[i8]: https://github.com/bilalahmad1995/WS2024-25-MADE
