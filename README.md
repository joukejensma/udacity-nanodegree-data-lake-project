# Steps taken during exercise (Project: Data Lake)

1. Jupyter notebook creation (a spark session will persist and make testing the ETL that much faster)
2. Take a look at the data folder - I unzipped the log-data and song-data files and put their contents data/song_data and data/log_data folders
3. I added my AWS credentials to dl.cfg
4. First, create local spark session and try to load the sample data in the etl_snippets.ipynb notebook
5. Convert column ts from milliseconds to timestamp type for log_data
6. No conversion seems to be required for the song_data
7. With the local loading taken care of, now is the time to set up queries that create the fact/dimension tables
8. I start with the time table, making sure that I select only distinct timestamps (unique values are no good)
9. Furthermore, I add a UDF for a column weekday which has value 1 for Mon-Fri, 0 Sat-Sun
10. I write the time table to parquet files locally to see if that works (it does)
11. Subsequent tables are written similarly and are straightforward. 
12. The songplay table is slightly more complicated as the song and log table need to be joined (on the artist name and song name), also filtered on page=NextSong
13. Now that the snippets work I try to fill in the etl.py file with the objective to store data locally and see whether it works without errors. The next step would be to store data on s3.
14. I added some logging info to etl.py to get some more information on duration of writing as writing to s3 seems to be really slow
15. I removed the boto3 module from etl.py and set up an EMR cluster, due to my usage of fstrings I had to specify the environment variable PYSPARK_PYTHON=/usr/bin/python3
16. I found that writing to a s3:// bucket instead of s3a:// was much faster (as examined in my AWS_HOST:18080 Apache Spark logging)
17. I could run my job with spark-submit etl.py on the AWS EMR command line. Running the script on an EMR cluster takes approx. 1 hour for the full data set.


# Answers to asked questions
## Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.
The sparkify company currently has locally hosted song and log data and is trying to scale up by moving the data into the cloud. The aim is to store the data on AWS S3, load it with AWS EMR (Spark), transform it into fact/dimension tables and return these to S3.
The advantage of this is no longer having to host on-premise servers with administrators and better scalability in the cloud.

## State and justify your database schema design and ETL pipeline.
The database design and ETL pipeline details are covered in the "Steps taken" section.

## [Optional] Provide example queries and results for song play analysis.
I wrote a Jupyter notebook that reads my parquet files from my own s3 bucket and tries to find the number of rows contained in the song_table file as an example.