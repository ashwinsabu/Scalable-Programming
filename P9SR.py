#! /usr/bin/env python
 
import datetime
from pyspark.sql import SparkSession

spark = SparkSession.builder \
      .config("spark.driver.host", "localhost") \
      .master("local[1]") \
      .appName("Top5") \
      .getOrCreate() 

# define a function to return extract the date .
def date_data(row):
    columns = row.split(" ")
    return(columns[2])



def map_date(date_data):
    date = date_data
    return (date, 1)


print(spark.sparkContext.textFile("BGL.log") \
      .map(date_data) \
      .map(map_date) \
      .reduceByKey(lambda x, y: x + y) \
      .takeOrdered(5, key=lambda x: -x[1]))
