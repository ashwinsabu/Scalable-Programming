#! /usr/bin/env python

import datetime
from pyspark.sql import SparkSession

spark = SparkSession.builder \
      .config("spark.driver.host", "localhost") \
      .master("local[1]") \
      .appName("Weather") \
      .getOrCreate() 

# define a function to return extract the date and temperature.
def date_and_temp(row):
    columns = row.split(",")
    if columns != None and columns[0] != "date":
        return (columns[0],columns[4]) 

# define a function to filter data to return only observations that fall on weekends
def is_weekend(date_and_temp):
    if date_and_temp is not None:
        day = int(date_and_temp[0][0:2])
        # the month name is the fourth to sixth  characters of the first part of the tuple
        month_name = date_and_temp[0][3:6]
        # convert the month name into a month number
        month_number = datetime.datetime.strptime(month_name, "%b").month
        # the year is in the eight to eleventh characters of the first part of the tuple
        year = int(date_and_temp[0][7:11])
        day_of_week = datetime.datetime(year, month_number, day).strftime('%A')
        return day_of_week in ["Saturday","Sunday"]
    else:
        return 0

# define a function to convert dates to week numbers
def map_dates_to_week_numbers(date_and_temp):
    date_and_time = date_and_temp[0]
    temperature = float(date_and_temp[1])
    day = int(date_and_time[0:2])
    # the month name is the fourth to sixth  characters of the first part of the tuple
    month_name = date_and_time[3:6]
    # convert the month name into a month number
    month_number = datetime.datetime.strptime(month_name, "%b").month
    # the year is in the eight to eleventh characters of the first part of the tuple
    year = int(date_and_time[7:11])
    day_of_week = datetime.datetime(year, month_number, day).strftime('%A')
    # convert the year month and day to a date and extract the week number
    week_number = datetime.datetime(year, month_number, day).isocalendar().week
    return (date_and_time[7:11]+"-"+str(week_number), temperature)


print(spark.sparkContext.textFile("hly532.csv") \
      .map(date_and_temp) \
      .filter(is_weekend) \
      .map(map_dates_to_week_numbers) \
      .mapValues(lambda temp: (temp, 1)) \
      .reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1])) \
      .mapValues(lambda x: x[0]/x[1]) \
      .filter(lambda x: x[1] < 0)
      .count())