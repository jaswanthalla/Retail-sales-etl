from pyspark.sql import SparkSession
# Create Spark Session
spark = SparkSession.builder \
    .appName("Retail Sales ETL Pipeline") \
    .getOrCreate()
# Read CSV File
df = spark.read.csv(
    "data/raw/sales.csv",
    header=True,
    inferSchema=True
)
# Show Data
df.show()
# Print Schema
df.printSchema()
# Total Rows
print("Total Rows:", df.count())
# Total Columns
print("Total Columns:", len(df.columns))
# Check Null Values
df.select("Quantity").show()
# Check Duplicate Rows
print("Duplicate Rows:", df.count() - df.dropDuplicates().count())
#Remove duplicate Rows
df=df.dropDuplicates()
print("Rows after removing duplicates:",df.count())
from pyspark.sql.functions import col
#Count Null Values
null_count=df.filter(col("Quantity").isNull()).count()
print("Null Values in Quantity:",null_count)
#Fill Null Values
df=df.fillna({"Quantity":1})
df.show()
print("Null after fill:",
      df.filter(col("Quantity").isNull()).count())
from pyspark.sql.functions import col
#Explicitly cast Quantity and Price
df=df.withColumn("Quantity",col("Quantity").cast("integer"))
df=df.withColumn("Price",col("Price").cast("double"))
df.printSchema()
#Create Total Amount column
df=df.withColumn("Total_Amount",col("Quantity")*col("Price"))
df.show()
from pyspark.sql.functions import month
#Extract month from Order_Date
df=df.withColumn("Order_Month",month(col("Order_Date")))
df.show()
df.printSchema()
df.show()
from pyspark.sql.functions import col,month,when
#Categorize orders based on Total_Amount
df=df.withColumn("Order_Size",when(col("Total_Amount")>=50000,
"High Value").otherwise("Normal"))
df.show()
from pyspark.sql.functions import year
#Order Year
df=df.withColumn("Order Year",year(col("Order_Date")))
df.show()
#Save cleaned and transformed data
output_path="data/processed/cleaned_sales"
df.write.mode("overwrite").parquet(output_path)
print("Cleaned data successfully saved.")
#Category-wise Sales
category_sales=df.groupBy("Category").sum("Total_Amount")
category_sales.show()
#Product-wise Sales
product_sales=df.groupBy("Product").sum("Total_Amount")
product_sales.show()
#City-wise Sales
city_sales=df.groupBy("City").sum("Total_Amount")
city_sales.show()
#Business KPIs(Key Performance Indicator)
#Total Revenue
total_revenue=df.agg({"Total_Amount":"sum"})
total_revenue.show()
#Total Orders
total_orders=df.count()
print("Total Orders:",total_orders)
#Total Quantity Sold
total_quantity=df.agg({"Quantity":"sum"})
total_quantity.show()
#Avg Order Value(total revenue/total orders)
aov=df.agg({"Total_Amount":"avg"})
aov.show()
#Top Product By Revenue
from pyspark.sql.functions import desc
top_product=(df.groupBy("Product").sum("Total_Amount")
             .orderBy(desc("sum(Total_Amount)")).limit(1))
print("Top Product by Revenue:")
top_product.show()
#Highest Revenue Category
top_category=(df.groupBy("Category").sum("Total_Amount").orderBy
              (desc("sum(Total_Amount)")).limit(1))
print("Top Category by Revenue:")
top_category.show()
#Window fn(Product-wise revenue ranking)
from pyspark.sql.window  import Window
from pyspark.sql.functions import row_number,desc
window_spec=Window.orderBy(desc("Total_Amount"))
ranked_df=df.withColumn("Revenue_Rank",row_number().over(window_spec))
ranked_df.select("Product","Total_Amount","Revenue_Rank").show()
#Ranking inside category
window_category=Window.partitionBy("Category").orderBy(desc("Total_Amount"))
category_ranked_df=df.withColumn("Category_Rank",row_number().over(window_category))
category_ranked_df.select("Category","Product","Total_Amount","Category_Rank").show()
#Spark SQL
df.createOrReplaceTempView("sales")
spark_sql_df=spark.sql("""
SELECT Category,SUM(Total_Amount)AS Revenue
FROM sales
GROUP BY Category
ORDER BY Revenue DESC""")
spark_sql_df.show()
#UDF to classify category on basis of product revenue
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
def revenue_category(amount):
    if amount>=50000:
        return "Premium"
    else:
        return "Standard"
revenue_udf=udf(revenue_category,StringType())
df=df.withColumn("Revenue_Category",revenue_udf(df["Total_Amount"]))
df.select("Product","Total_Amount","Revenue_Category").show()
# =========================================================
# SAVE ANALYTICS OUTPUTS
# =========================================================

# Category-wise Sales
category_sales.write.mode("overwrite").parquet(
    "data/analytics/category_sales"
)

# Product-wise Sales
product_sales.write.mode("overwrite").parquet(
    "data/analytics/product_sales"
)

# City-wise Sales
city_sales.write.mode("overwrite").parquet(
    "data/analytics/city_sales"
)

# Top Product by Revenue
top_product.write.mode("overwrite").parquet(
    "data/analytics/top_product"
)

# Top Category by Revenue
top_category.write.mode("overwrite").parquet(
    "data/analytics/top_category"
)

# Window Function - Overall Revenue Ranking
ranked_df.select(
    "Product",
    "Total_Amount",
    "Revenue_Rank"
).write.mode("overwrite").parquet(
    "data/analytics/revenue_ranking"
)

# Window Function - Category-wise Ranking
category_ranked_df.select(
    "Category",
    "Product",
    "Total_Amount",
    "Category_Rank"
).write.mode("overwrite").parquet(
    "data/analytics/category_ranking"
)

# Spark SQL Output
spark_sql_df.write.mode("overwrite").parquet(
    "data/analytics/spark_sql_sales"
)

# UDF Output
df.select(
    "Product",
    "Total_Amount",
    "Revenue_Category"
).write.mode("overwrite").parquet(
    "data/analytics/revenue_category"
)

print("All analytics outputs successfully saved.")