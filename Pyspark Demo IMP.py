# Databricks notebook source
fire_df= spark.read \
            .format('csv') \
            .option('header','true') \
            .option('inferSchema','true') \
            .load('/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv')

# COMMAND ----------

fire_df.show()

# COMMAND ----------

display(fire_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists demo_db

# COMMAND ----------



raw_fire_df = spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")
     

raw_fire_df.show(10)
     

display(raw_fire_df)
     

raw_fire_df.createGlobalTempView("fire_service_calls_view")
     


     


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.fire_service_calls_view limit 10

# COMMAND ----------

display(raw_fire_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Databricks notebook source
# MAGIC drop table if exists demo_db.fire_service_calls_tbl;
# MAGIC drop view if exists demo_db;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- MAGIC %fs rm -r /user/hive/warehouse/demo_db.db
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC create database if not exists demo_db;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC create table if not exists demo_db.fire_service_calls_tbl(
# MAGIC   CallNumber integer,
# MAGIC   UnitID string,
# MAGIC   IncidentNumber integer,
# MAGIC   CallType string,
# MAGIC   CallDate string,
# MAGIC   WatchDate string,
# MAGIC   CallFinalDisposition string,
# MAGIC   AvailableDtTm string,
# MAGIC   Address string,
# MAGIC   City string,
# MAGIC   Zipcode integer,
# MAGIC   Battalion string,
# MAGIC   StationArea string,
# MAGIC   Box string,
# MAGIC   OriginalPriority string,
# MAGIC   Priority string,
# MAGIC   FinalPriority integer,
# MAGIC   ALSUnit boolean,
# MAGIC   CallTypeGroup string,
# MAGIC   NumAlarms integer,
# MAGIC   UnitType string,
# MAGIC   UnitSequenceInCallDispatch integer,
# MAGIC   FirePreventionDistrict string,
# MAGIC   SupervisorDistrict string,
# MAGIC   Neighborhood string,
# MAGIC   Location string,
# MAGIC   RowID string,
# MAGIC   Delay float
# MAGIC ) using parquet;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC insert into demo_db.fire_service_calls_tbl 
# MAGIC values(1234, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 
# MAGIC null, null, null, null, null, null, null, null, null);
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC select * from demo_db.fire_service_calls_tbl;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC truncate table demo_db.fire_service_calls_tbl;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC insert into demo_db.fire_service_calls_tbl
# MAGIC select * from global_temp.fire_service_calls_view;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC select * from demo_db.fire_service_calls_tbl;
# MAGIC
# MAGIC -- COMMAND ----------

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Databricks notebook source
# MAGIC select * from demo_db.fire_service_calls_tbl limit 100;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC drop view if exists fire_service_calls_tbl_cache;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC cache lazy table fire_service_calls_tbl_cache as
# MAGIC select * from demo_db.fire_service_calls_tbl;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC select count(*) from demo_db.fire_service_calls_tbl;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- MAGIC %md
# MAGIC -- MAGIC ##### Q1. How many distinct types of calls were made to the Fire Department?
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC select count(distinct callType) as distinct_call_type_count
# MAGIC from demo_db.fire_service_calls_tbl
# MAGIC where callType is not null;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- MAGIC %md
# MAGIC -- MAGIC ##### Q2. What were distinct types of calls made to the Fire Department?
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC select distinct callType as distinct_call_types
# MAGIC from demo_db.fire_service_calls_tbl
# MAGIC where callType is not null;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- MAGIC %md
# MAGIC -- MAGIC ##### Q3. Find out all response for delayed times greater than 5 mins?
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC select callNumber, Delay
# MAGIC from demo_db.fire_service_calls_tbl
# MAGIC where Delay > 5;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- MAGIC %md
# MAGIC -- MAGIC ##### Q4. What were the most common call types?
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC select callType, count(*) as count
# MAGIC from demo_db.fire_service_calls_tbl
# MAGIC where callType is not null
# MAGIC group by callType
# MAGIC order by count desc;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- MAGIC %md
# MAGIC -- MAGIC ##### Q5. What zip codes accounted for most common calls?
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC select callType, zipCode, count(*) as count
# MAGIC from demo_db.fire_service_calls_tbl
# MAGIC where callType is not null
# MAGIC group by callType, zipCode
# MAGIC order by count desc;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- MAGIC %md
# MAGIC -- MAGIC ##### Q6. What San Francisco neighborhoods are in the zip codes 94102 and 94103?
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC select zipCode, neighborhood
# MAGIC from demo_db.fire_service_calls_tbl
# MAGIC where zipCode == 94102 or zipCode == 94103;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- MAGIC %md
# MAGIC -- MAGIC #####Q7. What was the sum of all call alarms, average, min, and max of the call response times?
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC select sum(NumAlarms), avg(Delay), min(Delay), max(Delay)
# MAGIC from demo_db.fire_service_calls_tbl;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- MAGIC %md
# MAGIC -- MAGIC ##### Q8. How many distinct years of data is in the data set?
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC select distinct year(to_date(callDate, "MM/dd/yyyy")) as year_num
# MAGIC from demo_db.fire_service_calls_tbl
# MAGIC order by year_num;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- MAGIC %md
# MAGIC -- MAGIC ##### Q9. What week of the year in 2018 had the most fire calls?
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC select weekofyear(to_date(callDate, "MM/dd/yyyy")) week_year, count(*) as count
# MAGIC from demo_db.fire_service_calls_tbl
# MAGIC where year(to_date(callDate, "MM/dd/yyyy")) == 2018
# MAGIC group by week_year
# MAGIC order by count desc;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- MAGIC %md
# MAGIC -- MAGIC ##### Q10. What neighborhoods in San Francisco had the worst response time in 2018?
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC select neighborhood, delay
# MAGIC from demo_db.fire_service_calls_tbl
# MAGIC where year(to_date(callDate, "MM/dd/yyyy")) == 2018
# MAGIC order by delay desc;
# MAGIC
# MAGIC -- COMMAND ----------

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

raw_fire_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema","true") \
    .load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")
     

# COMMAND ----------

display(raw_fire_df)

# COMMAND ----------

renamed_fire_df = raw_fire_df \
    .withColumnRenamed("Call Number", "CallNumber") \
    .withColumnRenamed("Unit ID", "UnitID") \
    .withColumnRenamed("Incident Number", "IncidentNumber") \
    .withColumnRenamed("Call Date", "CallDate") \
    .withColumnRenamed("Watch Date", "WatchDate") \
    .withColumnRenamed("Call Final Disposition", "CallFinalDisposition") \
    .withColumnRenamed("Available DtTm", "AvailableDtTm") \
    .withColumnRenamed("Zipcode of Incident", "Zipcode") \
    .withColumnRenamed("Station Area", "StationArea") \
    .withColumnRenamed("Final Priority", "FinalPriority") \
    .withColumnRenamed("ALS Unit", "ALSUnit") \
    .withColumnRenamed("Call Type Group", "CallTypeGroup") \
    .withColumnRenamed("Unit sequence in call dispatch", "UnitSequenceInCallDispatch") \
    .withColumnRenamed("Fire Prevention District", "FirePreventionDistrict") \
    .withColumnRenamed("Supervisor District", "SupervisorDistrict")
     

# COMMAND ----------

display(renamed_fire_df)

# COMMAND ----------

renamed_fire_df.printSchema()

# COMMAND ----------

fire_df = renamed_fire_df \
    .withColumn("CallDate", to_date("CallDate", "MM/dd/yyyy")) \
    .withColumn("WatchDate", to_date("WatchDate", "MM/dd/yyyy")) \
    .withColumn("AvailableDtTm", to_timestamp("AvailableDtTm", "MM/dd/yyyy hh:mm:ss a")) \
    .withColumn("Delay", round("Delay", 2))

# COMMAND ----------

display(fire_df)

# COMMAND ----------

fire_df.printSchema()

# COMMAND ----------

fire_df.cache()

# COMMAND ----------

fire_df.createOrReplaceTempView("fire_service_calls_view")
q1_sql_df = spark.sql("""
        select count(distinct CallType) as distinct_call_type_count
        from fire_service_calls_view
        where CallType is not null
        """)
display(q1_sql_df)

# COMMAND ----------

q1_df= fire_df.where('CallType is not null')\
        .select('CallType')\
        .distinct()
print(q1_df.count())

# COMMAND ----------

q2_df = fire_df.where("CallType is not null") \
            .select(expr("CallType as distinct_call_type")) \
            .distinct()
q2_df.show()

# COMMAND ----------

display(q2_df)

# COMMAND ----------

q3_df=fire_df.where("Delay>5")\
            .select("CallNumber","Delay")\
            .show()

# COMMAND ----------

fire_df.select("CallType") \
    .where("CallType is not null") \
    .groupBy("CallType") \
    .count() \
    .orderBy("count", ascending=False) \
    .show()

# COMMAND ----------

fire_df.select("CallType","ZipCode") \
    .where("CallType is not null") \
    .groupBy("CallType","ZipCode") \
    .count() \
    .orderBy("count", ascending=False) \
    .show()

# COMMAND ----------



# COMMAND ----------

fire_df.show(5)

# COMMAND ----------

fire_service_calls_df = fire_df.withColumn("CallDate", to_timestamp("CallDate", "MM/dd/yyyy"))

# Filter data for the year 2018
fire_service_calls_2018_df = fire_service_calls_df.filter(year("CallDate") == 2018)

# Group by weekofyear and count occurrences
calls_by_week_df = fire_service_calls_2018_df \
    .groupBy(weekofyear("CallDate").alias("week_year")) \
    .count() \
    .orderBy("count", ascending=False)

# Show the result
calls_by_week_df.show()

# COMMAND ----------

import os
os.environ['SPARK_HOME'] = "/Users/coder2j/Apps/Spark"
os.environ['PYSPARK_DRIVER_PYTHON'] = 'jupyter'
os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = 'lab'
os.environ['PYSPARK_PYTHON'] = 'python'

# COMMAND ----------

# Import PySpark
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder \
    .appName("PySpark-Get-Started") \
    .getOrCreate()

# COMMAND ----------

data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["Name", "Age"])
df.show()

# COMMAND ----------

from pyspark import SparkContext

# Create a SparkContext object
sc = SparkContext(appName="MySparkApplication")

# COMMAND ----------

sc

# COMMAND ----------

# sc.stop()

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("MySparkApplication") \
    .getOrCreate()

# Get the SparkContext from the SparkSession
sc = spark.sparkContext

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("MySparkApplication") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()


# COMMAND ----------

spark

# COMMAND ----------

numbers = [1, 2, 3, 4, 5]
rdd = spark.sparkContext.parallelize(numbers)

# COMMAND ----------

rdd.collect()

# COMMAND ----------

data = [("Alice", 25), ("Bob", 30), ("Charlie", 35), ("Alice", 40)]
rdd = spark.sparkContext.parallelize(data)

# COMMAND ----------

# Collect action: Retrieve all elements of the RDD
print("All elements of the rdd: ", rdd.collect())

# COMMAND ----------

# Count action: Count the number of elements in the RDD
count = rdd.count()
print("The total number of elements in rdd: ", count)

# COMMAND ----------

# First action: Retrieve the first element of the RDD
first_element = rdd.first()
print("The first element of the rdd: ", first_element)
# The first element of the rdd:  ('Alice', 25)
# # Take action: Retrieve the n elements of the RDD
taken_elements = rdd.take(2)
print("The first two elements of the rdd: ", taken_elements)

# COMMAND ----------

# Foreach action: Print each element of the RDD
rdd.foreach(lambda x: print(x))

# COMMAND ----------

rdd.collect()

# COMMAND ----------

# Map transformation: Convert name to uppercase
mapped_rdd = rdd.map(lambda x: (x[0].upper(), x[1]))
result = mapped_rdd.collect()
print("rdd with uppercease name: ", result)

# COMMAND ----------

# Filter transformation: Filter records where age is greater than 30
filtered_rdd = rdd.filter(lambda x: x[1] > 30)
filtered_rdd.collect()

# COMMAND ----------


# ReduceByKey transformation: Calculate the total age for each name
reduced_rdd = rdd.reduceByKey(lambda x, y: x + y)
reduced_rdd.collect()

# COMMAND ----------

# SortBy transformation: Sort the RDD by age in descending order
sorted_rdd = rdd.sortBy(lambda x: x[1], ascending=False)
sorted_rdd.collect()

# COMMAND ----------

# Save action: Save the RDD to a text file
rdd.saveAsTextFile("output.txt")

# COMMAND ----------

pwd

# COMMAND ----------

# create rdd from text file
rdd_text = spark.sparkContext.textFile("output.txt")
rdd_text.collect()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
# Create a SparkSession
spark = SparkSession.builder.appName("DataFrame-Demo").getOrCreate()

# COMMAND ----------

rdd = spark.sparkContext.textFile("./data/data.txt")
result_rdd = rdd.flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False)

# COMMAND ----------

# ---------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("BestOfThreeMarks").getOrCreate()

# Sample data
data = [
 (1, 85), (1, 78), (1, 90), (1, 88), (1, 92),
 (2, 76), (2, 65), (2, 80), (2, 85), (2, 77),
 (3, 95), (3, 88), (3, 91), (3, 89), (3, 87)
]

# COMMAND ----------

#Recently asked pyspark interview question
# You are given a student dataframe containing columns student_id and marks. Your task is to find out best of 3 marks using pyspark and avg of that for best of three? 

# My solution
# ---------------
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
# from pyspark.sql.window import Window

# # Initialize Spark Session
# spark = SparkSession.builder.appName("BestOfThreeMarks").getOrCreate()

# # Sample data
data = [
 (1, 85), (1, 78), (1, 90), (1, 88), (1, 92),
 (2, 76), (2, 65), (2, 80), (2, 85), (2, 77),
 (3, 95), (3, 88), (3, 91), (3, 89), (3, 87)
]

# Create DataFrame
columns = ["student_id", "marks"]
df = spark.createDataFrame(data, columns)
#define the window clause
spec=Window.partitionBy("student_id").orderBy(col("marks").desc())
#assigning ranks to the subjects for each students
df=df.withColumn("rank",rank().over(spec))
#filtering out best 3 marks for each student
filter_df=df.filter(df.rank<=3)
agg_df=filter_df.groupBy("student_id").agg(avg("marks").cast("int").alias("avg_marks"))
agg_df.show()

# COMMAND ----------

data

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("RDD-Demo").getOrCreate()
numbers=[1,2,3,4,5]
rdd=spark.sparkContext.parallelize(numbers)

# COMMAND ----------

rdd.collect()

# COMMAND ----------

count=rdd.count()
count

# COMMAND ----------

first_element=rdd.first()
first_element

# COMMAND ----------

data = [("Alice", 25), ("Bob", 30), ("Charlie", 35), ("Alice", 40)]
rdd = spark.sparkContext.parallelize(data)

# COMMAND ----------

rdd.first()

# COMMAND ----------

taken_elements = rdd.take(2)
taken_elements

# COMMAND ----------

rdd.foreach(lambda x: print(x))
rdd.collect()

# COMMAND ----------

elements = rdd.collect()
for element in elements:
    print(element)

# COMMAND ----------

mapped_rdd=rdd.map(lambda x: (x[0].upper(),x[1]))
mapped_rdd.collect()

# COMMAND ----------

filtered_rdd=rdd.filter(lambda x: x[1]>30)
filtered_rdd.collect()

# COMMAND ----------

reduced_rdd= rdd.reduceByKey(lambda x, y : x+y)
reduced_rdd.collect()

# COMMAND ----------

data = [("Alice", 25), ("Bob", 30), ("Charlie", 35), ("Alice", 40)]
rdd = spark.sparkContext.parallelize(data)

# Get the number of partitions
num_partitions = rdd.getNumPartitions()
print(f"Number of partitions: {num_partitions}")


# COMMAND ----------

reduced_rdd.saveAsTextFile("op_ac.txt")

# COMMAND ----------

import shutil
import os

output_dir = "op_ac.txt"

# Check if the directory exists
if os.path.exists(output_dir):
    # Delete the existing directory
    shutil.rmtree(output_dir)

# Save the RDD as a text file
# reduced_rdd.saveAsTextFile(output_dir)

# COMMAND ----------

# create rdd from text file
rdd_text = spark.sparkContext.textFile("op_ac.txt")
rdd_text.collect()

# COMMAND ----------

rdd = spark.sparkContext.textFile("op_ac.txt")
result_rdd = rdd.flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False)

# COMMAND ----------

result_rdd.collect()

# COMMAND ----------

from pyspark.sql.functions import desc

df = spark.read.text("dbfs:/op_ac.txt")

result_df = df.selectExpr("explode(split(value, ' ')) as word") \
    .groupBy("word").count().orderBy(desc("count"))

# COMMAND ----------



# COMMAND ----------

result_df.collect()

# COMMAND ----------

from pyspark.sql import SparkSession

# Step 1: Initialize Spark Session
spark = SparkSession.builder \
    .appName("CreateFilesExample") \
    .getOrCreate()

# Step 2: Create Sample DataFrames
# People DataFrame
people_data = [
    ("Alice", 25), ("Bob", 30), ("Charlie", 35), ("Alice", 40),
    ("David", 28), ("Edward", 45), ("Fiona", 23), ("George", 33),
    ("Hannah", 31), ("Ian", 50), ("Jack", 29), ("Karen", 41)
]
people_columns = ["Name", "Age"]
people_df = spark.createDataFrame(people_data, people_columns)

# Product DataFrame
product_data = [
    (1, "Laptop", 999.99), (2, "Smartphone", 799.49), (3, "Tablet", 499.99),
    (4, "Monitor", 199.99), (5, "Keyboard", 49.99), (6, "Mouse", 29.99),
    (7, "Printer", 149.99), (8, "Webcam", 89.99), (9, "Headphones", 69.99),
    (10, "Speakers", 119.99)
]
product_columns = ["ProductID", "ProductName", "Price"]
product_df = spark.createDataFrame(product_data, product_columns)

# Step 3: Write DataFrames to CSV, JSON, and Parquet Formats

# Write People DataFrame to CSV, JSON, and Parquet
people_df.write.csv("output/people.csv", header=True, mode="overwrite")
people_df.write.json("output/people.json", mode="overwrite")
people_df.write.parquet("output/people.parquet", mode="overwrite")

# Write Product DataFrame to CSV, JSON, and Parquet
product_df.write.csv("output/products.csv", header=True, mode="overwrite")
product_df.write.json("output/products.json", mode="overwrite")
product_df.write.parquet("output/products.parquet", mode="overwrite")

# # Stop the Spark session
# spark.stop()


# COMMAND ----------

csv_file_path = "dbfs:output/people.csv"
df = spark.read.csv(csv_file_path, header=True)

# COMMAND ----------

csv_file_path = "dbfs:/output/people.csv"
df = spark.read.csv(csv_file_path, header=True,inferSchema=True)

# COMMAND ----------

# Display schema of DataFrame
df.printSchema()

# Display content of DataFrame
df.show(5)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


# COMMAND ----------

json_file_path = "dbfs:/output/people.json"
df = spark.read.json(json_file_path, multiLine=True)

# COMMAND ----------

df.printSchema()

# Display content of DataFrame
df.show(5)

# COMMAND ----------

# write dataframe into parquet file


df = spark.read.parquet(parquet_file_path)

# COMMAND ----------


# Display schema of DataFrame
df.printSchema()

# Display content of DataFrame
df.show(5)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Step 1: Initialize Spark Session
spark = SparkSession.builder \
    .appName("CreateProductFilesExample") \
    .getOrCreate()

# Step 2: Define the schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True)
])

# Step 3: Create Sample Data
product_data = [
    (1, "iPhone", "Electronics", 10, 899.99),
    (2, "Macbook", "Electronics", 5, 1299.99),
    (3, "iPad", "Electronics", 15, 499.99),
    (4, "Samsung TV", "Electronics", 8, 799.99),
    (5, "LG TV", "Electronics", 10, 699.99),
    (6, "Nike Shoes", "Clothing", 30, 99.99),
    (7, "Adidas Shoes", "Clothing", 25, 89.99),
    (8, "Sony Headphones", "Electronics", 12, 149.99),
    (9, "Beats Headphones", "Electronics", 20, 199.99)
]

# Step 4: Create DataFrame
product_df = spark.createDataFrame(product_data, schema)

# Step 5: Write DataFrame to CSV, JSON, and Parquet Formats

# Write Product DataFrame to CSV
product_df.write.csv("output/products.csv", header=True, mode="overwrite")

# Write Product DataFrame to JSON
product_df.write.json("output/products.json", mode="overwrite")

# Write Product DataFrame to Parquet
product_df.write.parquet("output/products.parquet", mode="overwrite")



# COMMAND ----------

import shutil
import os

output_dir = "op_ac.txt"

# Check if the directory exists
if os.path.exists(output_dir):
    # Delete the existing directory
    shutil.rmtree(output_dir)

# Save the RDD as a text file
reduced_rdd.saveAsTextFile(output_dir)

# COMMAND ----------

# Display schema of DataFrame
product_df.printSchema()

# Show the initial DataFrame
print("Initial DataFrame:")
product_df.show(10)

# COMMAND ----------

selected_columns=product_df.select("id","name","price")
selected_columns.show()

# COMMAND ----------

csv_file_path = "dbfs:/output/products.csv"
df = spark.read.csv(csv_file_path, header=True,inferSchema=True)
df.show()

# COMMAND ----------

filtered_data=df.filter(df.quantity>10)
filtered_data.show()
df.count()

# COMMAND ----------

grouped_data=df.groupBy("category").agg({"quantity":"sum", "price":"avg"}).alias("sum_quantity")
grouped_data.show()

# COMMAND ----------

df2=df.select("id","category").limit(10)
joined_data=df.join(df2, "id","inner")
joined_data.show()

# COMMAND ----------

sorted_data=df.orderBy("price")
sorted_data.show()

# COMMAND ----------

from pyspark.sql.functions import col, desc
sorted_data = df.orderBy(col("price").desc(), col("id").desc())
print("Sorted Data Descending:")
sorted_data.show(10)

# COMMAND ----------

distinct_rows=df.select("category").distinct()
distinct_rows.show()

# COMMAND ----------

dropped_columns=df.drop("quantity","category")
dropped_columns.show()

# COMMAND ----------

df_new_col=df.withColumn("total_price",df.quantity*df.price).withColumnRenamed("price","product_price")
df_new_col.show()

# COMMAND ----------

df.createOrReplaceTempView("products")

# COMMAND ----------

result=spark.sql("select * from products where quantity>20")
result.show()

# COMMAND ----------

avg_sal_by_gen=spark.sql("select category, avg(price) from products group by category")
avg_sal_by_gen.show()

# COMMAND ----------

view_exists = spark.catalog.tableExists("products")
view_exists

# COMMAND ----------

spark.catalog.dropTempView("products")

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

# Create DataFrames
employee_data = [
    (1, "John"), (2, "Alice"), (3, "Bob"), (4, "Emily"),
    (5, "David"), (6, "Sarah"), (7, "Michael"), (8, "Lisa"),
    (9, "William")
]
employees = spark.createDataFrame(employee_data, ["id", "name"])

salary_data = [
    ("HR", 1, 60000), ("HR", 2, 55000), ("HR", 3, 58000),
    ("IT", 4, 70000), ("IT", 5, 72000), ("IT", 6, 68000),
    ("Sales", 7, 75000), ("Sales", 8, 78000), ("Sales", 9, 77000)
]
salaries = spark.createDataFrame(salary_data, ["department", "id", "salary"])

employees.show()

salaries.show()

# COMMAND ----------

employees.createOrReplaceTempView("employees")
salaries.createOrReplaceTempView("salaries")

# COMMAND ----------

# Subquery to find employees with salaries above average
result = spark.sql("""
    SELECT name
    FROM employees
    WHERE id IN (
        SELECT id
        FROM salaries
        WHERE salary > (SELECT AVG(salary) FROM salaries)
    )
""")

result.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import functions as F

# COMMAND ----------

emp_sal=spark.sql("select * from employees left join salaries on salaries.id = employees.id ")
emp_sal.show()

# COMMAND ----------

window_spec=Window.partitionBy("department").orderBy(F.desc("salary"))

# COMMAND ----------

emp_sal.withColumn("rank",F.rank().over(window_spec)) \
       .withColumn("dense_rank",F.dense_rank().over(window_spec)) \
       .withColumn("rank1",F.row_number().over(window_spec)) \
       .show()

# COMMAND ----------


