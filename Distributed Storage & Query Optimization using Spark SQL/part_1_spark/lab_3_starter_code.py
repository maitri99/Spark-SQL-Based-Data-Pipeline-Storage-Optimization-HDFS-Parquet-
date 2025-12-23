#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Starter Pyspark Script for students to complete for their Lab 3 Assignment.
Usage:
    $ spark-submit lab_3_starter_code.py
'''
import os

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession


def main(spark, userID):
    '''Main routine for Lab Solutions
    Parameters
    ----------
    spark : SparkSession object
    userID : string, userID of student to find files in HDFS
    '''
    print('Lab 3 Example dataframe loading and SQL query')

    # Load the boats.txt and sailors.json data into DataFrame
    boats = spark.read.csv(f'hdfs:/user/{userID}/boats.txt')
    sailors = spark.read.json(f'hdfs:/user/{userID}/sailors.json')
    reserves = spark.read.json(f'hdfs:/user/{userID}/reserves.json')

    print('Printing boats inferred schema')
    boats.printSchema()
    print('Printing sailors inferred schema')
    sailors.printSchema()
    # Why does sailors already have a specified schema?

    print('Reading boats.txt and specifying schema')
    boats = spark.read.csv('boats.txt', schema='bid INT, bname STRING, color STRING')

    print('Printing boats with specified schema')
    boats.printSchema()

    # Give the dataframe a temporary view so we can run SQL queries
    boats.createOrReplaceTempView('boats')
    sailors.createOrReplaceTempView('sailors')
    reserves.createOrReplaceTempView('reserves')
    # Construct a query
    print('Example 1: Executing SELECT count(*) FROM boats with SparkSQL')
    query = spark.sql('SELECT count(*) FROM boats')

    # Print the results to the console
    query.show()

    #####--------------YOUR CODE STARTS HERE--------------#####

    #make sure to load reserves.json, artist_term.csv, and tracks.csv
    #For the CSVs, make sure to specify a schema!

    reserves = spark.read.json(f'hdfs:/user/{userID}/reserves.json')
    reserves.createOrReplaceTempView('reserves')

    artist_term_schema = 'artistID STRING, term STRING'
    artist_term = spark.read.csv(f'hdfs:/user/{userID}/artist_term.csv', schema=artist_term_schema, header=True)
    artist_term.createOrReplaceTempView('artist_term')

    tracks_schema = 'trackID STRING, title STRING, release STRING, year INT, duration FLOAT, artistID STRING'
    tracks = spark.read.csv(f'hdfs:/user/{userID}/tracks.csv', schema=tracks_schema, header=True)
    tracks.createOrReplaceTempView('tracks')

    # Question 1
    print("Q1: SQL version of filter + select")
    q1 = spark.sql('SELECT sid, sname, age FROM sailors WHERE age > 40')
    q1.show()

    # Question 2
    print("Q2: Object version of SQL query")
    q2 = reserves.filter(reserves.bid != 101).groupBy("sid").agg(F.count("bid").alias("count_bid"))
    q2.show()

    # Question 3
    print("Q3: Distinct boats reserved per sailor")
    q3 = spark.sql('''
        SELECT s.sid, first(s.sname) as name, COUNT(DISTINCT r.bid) as distinct_boats
        FROM sailors s
        JOIN reserves r ON s.sid = r.sid
        GROUP BY s.sid
    ''')
    q3.show()

    # 1.6
    print('Reading artist_term.csv and specifying schema')
    artist_term = spark.read.csv('artist_term.csv', schema='artistID STRING, term STRING')

    print('Printing artist_term with specified schema')
    artist_term.printSchema()

    print('Reading tracks.csv and specifying schema')
    tracks = spark.read.csv('tracks.csv', schema='trackID STRING, title STRING, release STRING, year INT, duration FLOAT, artistID STRING')

    print('Printing tracks with specified schema')
    tracks.printSchema()

    # Question 4
    print("Q4: Stats per artist term")

    joined = artist_term.join(tracks, on="artistID", how="inner")

    # Group by term and compute:
    # - median year using percentile_approx
    # - max duration
    # - count of distinct artists
    # - average duration 
    aggregated_df = joined.groupBy("term").agg(
        F.percentile_approx("year", 0.5).alias("median_year"),
        F.max("duration").alias("max_duration"),
        F.countDistinct("artistID").alias("num_artists"),
        F.avg("duration").alias("avg_duration")  # for filtering top 10
    )

    # Order by average duration and take the top 10
    q4 = aggregated_df.orderBy("avg_duration").select("term", "median_year", "max_duration", "num_artists", "avg_duration").limit(10)
    q4.show()

    # Question 5
    print("Q5: Track count per term")
    
    # Count distinct tracks per term
    track_counts = joined.groupBy("term").agg(F.countDistinct("trackID").alias("num_tracks"))

    # Top 10 terms
    q5_top_10_terms = track_counts.orderBy(F.desc("num_tracks")).limit(10)
    print("Top 10 Most Popular Terms by Track Count")
    q5_top_10_terms.show()

    # Bottom 10 terms
    q5_bottom_10_terms = track_counts.orderBy(F.asc("num_tracks")).limit(10)
    print("Bottom 10 Least Popular Terms by Track Count")
    q5_bottom_10_terms.show()

# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part1').getOrCreate()

    # Get user userID from the command line
    # We need this to access the user's folder in HDFS
    userID = os.environ['USER']

    # Call our main routine
    main(spark, userID)
