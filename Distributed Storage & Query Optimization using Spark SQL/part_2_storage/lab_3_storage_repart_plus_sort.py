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

 

    # Convert peopleBig.csv to Parquet
    df_big = spark.read.csv('hdfs:/user/pw44_nyu_edu/peopleBig.csv', schema=schema, header=False)

    # Sort by zipcode and repartition by zipcode
    df_big_sorted = df_big.sort("zipcode").repartition("zipcode")

    # Write Parquet file with the combination optimization
    df_big_sorted.write.mode('overwrite').parquet('hdfs:/user/jg8316_nyu_edu/people_big_repart_plus_sort.parquet')

    print("Wrote people_big_repart_plus_sort.parquet (sorted and repartitioned by zipcode)")



# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part2').getOrCreate()

    #If you wish to command line arguments, look into the sys library(primarily sys.argv)
    #Details are here: https://docs.python.org/3/library/sys.html
    #If using command line arguments, be sure to add them to main function

    main(spark)
    
