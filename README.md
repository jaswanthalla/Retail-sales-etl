# Retail Sales ETL Pipeline

An end-to-end Data Engineering ETL pipeline built using PySpark to process, clean, transform, analyze, and store retail sales data.

## Project Overview

This project demonstrates a complete ETL workflow using PySpark.

Raw retail sales data is read from a CSV file, validated and cleaned, transformed into an analysis-ready dataset, and stored in Parquet format.

The pipeline also performs business analytics using PySpark DataFrame operations, Window Functions, Spark SQL, and a User Defined Function (UDF).

---

## Tech Stack

- Python
- PySpark
- Spark SQL
- Parquet
- Git / GitHub

---

## Project Type

End-to-End Data Engineering ETL Pipeline

---

## Dataset

Retail Sales Data containing information such as:

- Order ID
- Customer ID
- Product
- Category
- Quantity
- Price
- Order Date
- City
- State

The raw dataset is stored as:

`data/raw/sales.csv`

---

## ETL Flow

```text
Raw CSV
   |
   v
Extract
   |
   v
Data Validation
   |
   v
Data Cleaning
   |
   +--> Remove Duplicate Rows
   |
   +--> Check Null Values
   |
   +--> Fill Missing Quantity
   |
   v
Data Transformation
   |
   +--> Cast Data Types
   |
   +--> Calculate Total Amount
   |
   +--> Extract Order Month
   |
   +--> Extract Order Year
   |
   +--> Categorize Order Size
   |
   v
Processed Parquet Dataset
   |
   v
Business Analytics
   |
   +--> Category-wise Sales
   +--> Product-wise Sales
   +--> City-wise Sales
   +--> Total Revenue
   +--> Total Orders
   +--> Total Quantity Sold
   +--> Average Order Value
   +--> Top Product
   +--> Top Category
   |
   v
Advanced Analytics
   |
   +--> Window Functions
   +--> Spark SQL
   +--> UDF
   |
   v
Analytics Parquet Outputs
Data Cleaning
The pipeline performs the following data quality operations:
Checks total rows and columns
Checks null values
Detects duplicate rows
Removes duplicate rows
Fills missing values in the Quantity column
Verifies that null values are removed
Explicitly casts Quantity to integer
Explicitly casts Price to double
Data Transformations
Total Amount
A new column Total_Amount is created using:
Total_Amount = Quantity × Price
Order Month
The month is extracted from Order_Date.
Order Year
The year is extracted from Order_Date.
Order Size
Orders are classified based on revenue:
Total Amount >= 50000  → High Value
Otherwise              → Normal
Business Analytics
The pipeline generates:
Category-wise Sales
Calculates total revenue for each product category.
Product-wise Sales
Calculates total revenue for each product.
City-wise Sales
Calculates total revenue for each city.
Business KPIs
The pipeline calculates:
Total Revenue
Total Orders
Total Quantity Sold
Average Order Value
Top Product by Revenue
Top Category by Revenue
Window Functions
Window functions are used to calculate rankings while preserving individual rows.
Overall Revenue Ranking
Products/orders are ranked based on Total_Amount.
Category-wise Ranking
Products are ranked within their respective categories using:
PARTITION BY Category
ORDER BY Total_Amount DESC
This demonstrates how Window Functions can perform partition-based analysis without collapsing the original rows.
Spark SQL
The transformed DataFrame is registered as a temporary SQL view:
sales
Spark SQL is then used to perform category-wise revenue analysis.
Example:
SELECT Category,
       SUM(Total_Amount) AS Revenue
FROM sales
GROUP BY Category
ORDER BY Revenue DESC;
User Defined Function (UDF)
A PySpark UDF is used to classify products based on revenue.
Total Amount >= 50000  → Premium
Otherwise              → Standard
This demonstrates how custom business logic can be applied when required.
Output Format
The processed data and analytics results are stored in Parquet format.
Processed Data
data/processed/cleaned_sales/
Analytics Outputs
data/analytics/
├── category_sales/
├── product_sales/
├── city_sales/
├── top_product/
├── top_category/
├── revenue_ranking/
├── category_ranking/
├── spark_sql_sales/
└── revenue_category/
Parquet is used as the output format for efficient analytical data storage.
Project Structure
Retail-Sales-ETL/
│
├── data/
│   ├── raw/
│   │   └── sales.csv
│   │
│   ├── processed/
│   │   └── cleaned_sales/
│   │
│   └── analytics/
│       ├── category_sales/
│       ├── product_sales/
│       ├── city_sales/
│       ├── top_product/
│       ├── top_category/
│       ├── revenue_ranking/
│       ├── category_ranking/
│       ├── spark_sql_sales/
│       └── revenue_category/
│
├── src/
│
├── main.py
├── README.md
├── requirements.txt
└── .gitignore
How to Run
1. Clone the repository
git clone https://github.com/Divy-dev/retail-sales-etl
2. Navigate to the project
cd Retail-Sales-ETL
3.Install Dependencies
pip install -r requirements.txt
4. Run the ETL pipeline
python main.py
The pipeline will process the raw CSV data and generate the processed and 
analytical Parquet outputs.
Key Data Engineering Concepts Demonstrated
ETL Pipeline
Data Quality Checks
Duplicate Handling
Null Handling
Data Type Casting
Data Transformation
Parquet Storage
Aggregations
Business KPI Generation
Window Functions
Partitioning
Ranking
Spark SQL
Temporary Views
User Defined Functions (UDF)
Author
Divyansh Dubey
Data Engineering / PySpark Project
