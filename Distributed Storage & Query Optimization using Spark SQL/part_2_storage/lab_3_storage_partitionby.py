#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Template script to connect to Active Spark Session
Usage:
    $ spark-submit --deploy-mode client lab_3_storage_template_code.py <any arguments you wish to add>
'''


# Import command line arguments and helper functions(if necessary)
import sys

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession



def main(spark):
    '''Main routine for run for Storage optimization template.
    Parameters
    ----------
    spark : SparkSession object

    '''

    schema = 'first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN'

    # Convert peopleSmall.csv to Parquet
    df_small = spark.read.csv('hdfs:/user/pw44_nyu_edu/peopleSmall.csv', schema=schema, header=False)
    df_small.write.mode('overwrite').parquet('hdfs:/user/jg8316_nyu_edu/people_small.parquet')
    print("Wrote people_small.parquet")

    # Convert peopleModerate.csv to Parquet
    df_mod = spark.read.csv('hdfs:/user/pw44_nyu_edu/peopleModerate.csv', schema=schema, header=False)
    df_mod.write.mode('overwrite').parquet('hdfs:/user/jg8316_nyu_edu/people_moderate.parquet')
    print("Wrote people_moderate.parquet")

    # Convert peopleBig.csv to Parquet
    df_big = spark.read.csv('hdfs:/user/pw44_nyu_edu/peopleBig.csv', schema=schema, header=False)

    # Optimization: partitionBy zipcode
    df_big.write.mode('overwrite').partitionBy("zipcode").parquet('hdfs:/user/jg8316_nyu_edu/people_big_partitionby.parquet')

    print(" Wrote people_big_partitionby.parquet (partitioned by zipcode)")



# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part2').getOrCreate()

    #If you wish to command line arguments, look into the sys library(primarily sys.argv)
    #Details are here: https://docs.python.org/3/library/sys.html
    #If using command line arguments, be sure to add them to main function

    main(spark)
    
