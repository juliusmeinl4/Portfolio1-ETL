# ETL Pipeline for E-commerce Sales Data
## Project Overview

This ETL pipeline is designed for efficient processing of e-commerce sales data from various sources, including Walmart, Houzz, Faire, and Wayfair APIs, as well as local files. It standardizes, cleans, and aggregates sales data, updating inventory levels to assist in sales analysis and inventory management. The project addresses the challenge of integrating data across different sales channels and handling complex SKU relationships, especially for products sold as sets but tracked individually in inventory.
### Features
    Automated Data Extraction: Seamlessly extracts data from multiple e-commerce platforms and local files.
    Data Transformation: Cleans and standardizes data to ensure consistency across different sources.
    Inventory Management: Updates inventory levels based on aggregated sales data.
    SKU Mapping: Handles complex SKU relationships, mapping wholesale SKUs to retail SKUs for accurate reporting.

### Technologies Used
    Python 3.11
    Pandas for data manipulation
    Requests for API interactions
    XML and JSON for data formatting
    PostgreSQL for production-level data storage
    Subprocess for executing curl commands

### Production vs. Portfolio Differences
For production, this project utilizes PostgreSQL to manage and store data efficiently. However, for portfolio purposes and ease of demonstration, it operates on flat files for SKU mapping and data storage. This approach showcases the pipeline's core functionalities without the need for a database setup.

### Challenges Addressed
    Data Integration: Harmonizing data from diverse sales channels into a uniform format.
    Complex SKU Management: Dealing with products sold as sets (e.g., towel sets) while managing inventory at an individual item level, requiring intricate SKU mapping to differentiate between 
