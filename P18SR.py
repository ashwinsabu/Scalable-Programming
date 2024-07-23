#! /usr/bin/env python

import datetime
from pyspark.sql import SparkSession

spark = SparkSession.builder \
      .config("spark.driver.host", "localhost") \
      .master("local[1]") \
      .appName("FatalError") \
      .getOrCreate() 

# FF=uction to return date and time column from the 5th column 
def date_time(row):
    columns = row.split(" ")
    return columns[4]

# Function to check the message after merging the 10th column
def contains_error(row):
    s = row.split(" ")
    # the first eight elements in the split are the first eight columns
    cols = s[0:9]
    # everything after the first eight elements is the nineth column
    cols.append(" ".join(s[9:len(s)]))
    return "Lustre mount FAILED" in cols[9]

# Main script to find earliest fatal error with "Lustre mount FAILED"
earliest_error = spark.sparkContext.textFile("BGL.log") \
                    .filter(contains_error) \
                    .map(lambda row: (date_time(row), row)) \
                    .reduce(lambda x, y: x if datetime.datetime.strptime(x[0], "%Y-%m-%d-%H.%M.%S.%f") < datetime.datetime.strptime(y[0], "%Y-%m-%d-%H.%M.%S.%f") else y)

# Print the result
print("Earliest fatal error with 'Lustre mount FAILED' occurred on date and time:", earliest_error[0])
# print("Log message:", earliest_error[1])
