# Spark and Parquet Optimization Report

## Part 1: Spark

#### Question 1: 
How would you express the following computation using SQL instead of the object interface: `sailors.filter(sailors.age > 40).select(sailors.sid, sailors.sname, sailors.age)`?

Code:
```SQL

SELECT sid, sname, age FROM sailors WHERE age > 40

```


Output:
```

+---+-------+----+
|sid|  sname| age|
+---+-------+----+
| 22|dusting|45.0|
| 31| lubber|55.5|
| 95|    bob|63.5|
+---+-------+----+

```


#### Question 2: 
How would you express the following using the object interface instead of SQL: `spark.sql('SELECT sid, COUNT(bid) from reserves WHERE bid != 101 GROUP BY sid')`?

Code:
```python

reserves.filter(reserves.bid != 101).groupBy("sid").agg(F.count("bid").alias("count_bid"))

```


Output:
```

+---+---------+
|sid|count_bid|
+---+---------+
| 64|        1|
| 74|        1|
| 22|        3|
| 31|        3|
+---+---------+

```

#### Question 3: 
Using a single SQL query, how many distinct boats did each sailor reserve? 
The resulting DataFrame should include the sailor's id, name, and the count of distinct boats. 
(Hint: you may need to use `first(...)` aggregation function on some columns.) 
Provide both your query and the resulting DataFrame in your response to this question.

Code:
```SQL

q3 = spark.sql('''
    SELECT s.sid, first(s.sname) as name, COUNT(DISTINCT r.bid) as distinct_boats
    FROM sailors s
    JOIN reserves r ON s.sid = r.sid
    GROUP BY s.sid
''')
q3.show()

```


Output:
```

+---+-------+--------------+
|sid|   name|distinct_boats|
+---+-------+--------------+
| 22|dusting|             4|
| 31| lubber|             3|
| 64|horatio|             2|
| 74|horatio|             1|
+---+-------+--------------+

```

#### Question 4: 
Implement a query using Spark transformations which finds for each artist term, compute the median year of release, maximum track duration, and the total number of artists for that term (by ID).
  What are the results for the ten terms with the shortest *average* track durations?
  Include both your query code and resulting DataFrame in your response.


Code:
```python

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

```


Output:
```

+----------------+-----------+------------+-----------+------------------+
|            term|median_year|max_duration|num_artists|      avg_duration|
+----------------+-----------+------------+-----------+------------------+
|       mope rock|          0|    13.66159|          1|13.661589622497559|
|      murder rap|          0|    15.46404|          1| 15.46403980255127|
|    abstract rap|       2000|    25.91302|          1| 25.91301918029785|
|experimental rap|       2000|    25.91302|          1| 25.91301918029785|
|  brutal rapcore|          0|    26.46159|          1|26.461589813232422|
|     ghetto rock|          0|    26.46159|          1|26.461589813232422|
|     punk styles|          0|    41.29914|          1| 41.29914093017578|
|     turntablist|       1993|   145.89342|          1| 43.32922387123108|
| german hardcore|          0|    45.08689|          1|45.086891174316406|
|     noise grind|       2005|    89.80853|          2| 47.68869247436523|
+----------------+-----------+------------+-----------+------------------+

```
#### Question 5: 
Create a query using Spark transformations that finds the number of distinct tracks associated (through artistID) to each term.
  Modify this query to return only the top 10 most popular terms, and again for the bottom 10.
  Include each query and the tables for the top 10 most popular terms and the 10 least popular terms in your response. 

Code:
```python
# Count distinct tracks per term
track_counts = joined.groupBy("term").agg(F.countDistinct("trackID").alias("num_tracks")) #joined taken from q4 query

# Top 10 terms
q5_top_10_terms = track_counts.orderBy(F.desc("num_tracks")).limit(10)
print("Top 10 Most Popular Terms by Track Count")
q5_top_10_terms.show()

# Bottom 10 terms
q5_bottom_10_terms = track_counts.orderBy(F.asc("num_tracks")).limit(10)
print("Bottom 10 Least Popular Terms by Track Count")
q5_bottom_10_terms.show()
```

Output:
```
Top 10 Most Popular Terms by Track Count
+----------------+----------+
|            term|num_tracks|
+----------------+----------+
|            rock|     21796|
|      electronic|     17740|
|             pop|     17129|
|alternative rock|     11402|
|         hip hop|     10926|
|            jazz|     10714|
|   united states|     10345|
|        pop rock|      9236|
|     alternative|      9209|
|           indie|      8569|

Bottom 10 Least Popular Terms by Track Count
+--------------------+----------+
|                term|num_tracks|
+--------------------+----------+
|          freakdance|         1|
|               porto|         1|
|       mexican divas|         1|
|    alternative jazz|         1|
|              bailar|         1|
|              denton|         1|
|          neoambient|         1|
| krautrock influence|         1|
|slam brutal death...|         1|
|          micromusic|         1|
```

## Part 2: Parquet Optimization:

What to include in your report:

### csv_big_spender Benchmark Results
# 2.3 
| Dataset Size | Storage Format | Min Time (s) | Median Time (s) | Max Time (s) |
|--------------|----------------|--------------|------------------|--------------|
| Small        | CSV            | 0.177        | 0.255            | 6.019        | csv_big_spender 
| Moderate     | CSV            | 0.849        | 0.950            | 6.439        | csv_big_spender 
| Big          | CSV            | 76.738       | 82.312           | 90.045       | csv_big_spender 
| Small        | CSV            | 0.158        | 0.255            | 6.656        | csv_brian
| Moderate     | CSV            | 0.856        | 1.017            | 7.913        | csv_brian
| Big          | CSV            | 64.637       | 66.680           | 125.519      | csv_brian
| Small        | CSV            | 0.224        | 0.308            | 6.243        | csv_sum_order
| Moderate     | CSV            | 2.317        | 2.413            | 11.170       | csv_sum_order
| Big          | CSV            | 77.508       | 78.695           | 84.926       | csv_sum_order

# 2.4
| Dataset Size | Storage Format | Min Time (s) | Median Time (s) | Max Time (s) |
|--------------|----------------|--------------|------------------|--------------|
| Small        | Parquet        | 0.173        | 0.245            | 3.618        | parquet_big_spender
| Moderate     | Parquet        | 0.161        | 0.229            | 6.325        | parquet_big_spender
| Big          | Parquet        | 1.896        | 1.963            | 10.743       | parquet_big_spender
| Small        | Parquet        | 0.150        | 0.213            | 6.950        | parquet_brian
| Moderate     | Parquet        | 0.179        | 0.266            | 6.494        | parquet_brian
| Big          | Parquet        | 5.720        | 6.386            | 11.771       | parquet_brian
| Small        | Parquet        | 0.211        | 0.279            | 3.945        | parquet_sum_order
| Moderate     | Parquet        | 0.482        | 0.613            | 8.286        | parquet_sum_order
| Big          | Parquet        | 8.142        | 8.841            | 13.806       | parquet_sum_order

2.5 (Big Data Set Only)
# 1st Optimisation --> Repartition

| Dataset Size   | Storage Format              | Min Time (s) | Median Time (s) | Max Time (s) |
|----------------|-----------------------------|--------------|-----------------|--------------|
| pq_big_spender | repartition(zipcode) + sort | 0.907        | 0.978           | 6.027        |
| pq_brian       | repartition(zipcode) + sort | 3.053        | 3.945           | 8.352        |
| pq_sum_orders  | repartition(zipcode) + sort | 3.518        | 3.904           | 12.562       |


  - *How do the results in parts 2.3, 2.4, and 2.5 compare?*

  - In **Part 2.3:**  CSV files showed the slowest performance, especially on the big dataset, with median query times reaching over 82s for csv_big_spender and 66s for csv_brian. This was probably because of the lack of schema, indexing, and row-based format, which led to poor performance and longer scan times.

  - **In Part 2.4:** converting to Parquet significantly improved performance. On the big dataset, median times dropped to ~1.96s (parquet_big_spender), 6.39s (parquet_brian), and 8.84s (parquet_sum_orders) — over 10x faster than compared to their respective CSV counterparts, Parquet's columnar format and that it was able to smartly skip data uring read time lead to faster queries and lower source usage.

  - **In Part 2.5:** applying repartitioning and sorting by zipcode further reduced median times: down to ~0.98s for pq_big_spender, ~3.95s for pq_brian, and ~3.90s for pq_sum_orders. This optimization improved scan locality and parallelization, making it the most efficient among the three approaches.

  - *What did you try in part 2.5 to improve performance for each query?*
    - Repartition by zipcode: Applied df.repartition("zipcode") before writing to Parquet to redistribute the dataset across Spark partitions based on the zipcode key. This was meant to help Apark skip the irrelevant paritions when filtering with Zipcode 

    - PartitionBy Zipcode: Used .write.partitionBy("zipcode") to write the Parquet files into directory-based partitions. This aimed to allow apark to leverage parrition pruning to avoid unecessary file reads when filtering on the parition column. 

    - Sort by Zipcode before write: Used df.sortWithinPartitions("zipcode") prior to saving the DataFrame. This aimed to imporve the I/O effeiciency during scans and and tried to ensure intra-partition ordering of records by zipcode. 

    - Repartiion + Sort: Finally, we also tried combining repartitioning by zipcode and sorting wihtin paritions. This aimed to achieve both scan locality and local ordering within the paritions and imporvev the compression and the query speed. 
    
  - *What worked, and what didn't work?*
    - Repartitioning by zipcode provided a good consistent performance improvement across all queries over the baseline files HIt was able to optimize data layout for distributed processing and minimize shuffle during aggregation, leading to significant speedups, but it was not able to, alone, significantly help compression or ordering.

    - PartitionBy did not yield the best results. While it helped with partition pruning in selective filters, it introduced substantial write-time overheads and created a large number of small files, which negatively impacted query planning and execution.

    - Sorting within partitions alone did not perfrom the best. Since it did not alter the partitioning scheme or reduce data movement, it mostly led to lsightly more effeicint reads, with no major impact on the time it took for the queries to run.

    - Finally, Repartitioning + sorting worked the best, and gace the best results. It was effeicint in filtersinf from the partitioning and improved compression adn locality from the sorting. It was able to perform well on the Big data set. 

Basic Markdown Guide: https://www.markdownguide.org/basic-syntax/


