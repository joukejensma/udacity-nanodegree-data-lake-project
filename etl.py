from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from pyspark.sql.types import *

#import boto3
import configparser
import os
import time

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Utility function to create spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    # enabling this reduces write time for a subset from 3.7 min to 3.6 min, within margin of error as it seems to be enabled by default these days 
    # spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Using spark context, read song data from specified folder 'input_data' and write fact/dimension tables to location 'output_data'.
    """

    # get filepath to song data file
    # song_data_path = f"{input_data}song_data/A/A/A/*.json"
    song_data_path = f"{input_data}song_data/*/*/*/*.json"
    
    # read song data file

    # schema source: see notebook
    song_data_schema = StructType([
        StructField("artist_id", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("artist_longitude", DoubleType()),
        StructField("artist_name", StringType()),
        StructField("duration", DoubleType()),
        StructField("num_songs", LongType()),
        StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("year", LongType()),
    ])

    # read with explicitly specified schema
    song_data = spark.read \
        .schema(song_data_schema) \
        .option('mode', 'DROPMALFORMED') \
        .json(song_data_path)
    
    # extract columns to create songs table
    spark.sparkContext.setJobGroup("Select", "Selecting songs_table")
    songs_table = song_data.select('song_id',\
                            'title',\
                            'artist_id',\
                             'year',\
                             'duration')\
                            .drop_duplicates(subset = ['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    t1 = time.time()
    spark.sparkContext.setJobGroup("Write", "Writing songs_table")
    songs_table.write.mode("overwrite").partitionBy(['year', 'artist_id']).save(f'{output_data}songs_table.parquet')
    t2 = time.time()
    print(f"Writing songs_table took {(t2 - t1):.2f} seconds!")

    # extract columns to create artists table
    spark.sparkContext.setJobGroup("Select", "Selecting artist_table")
    artists_table = song_data.select('artist_id',\
                           F.col('artist_name').alias("name"),\
                           F.col('artist_location').alias("location"),\
                           F.col('artist_latitude').alias('latitude'),\
                           F.col('artist_longitude').alias('longitude'))\
                    .drop_duplicates(subset = ['artist_id'])
    
    # write artists table to parquet files
    t1 = time.time()
    spark.sparkContext.setJobGroup("Write", "Writing artist_table")
    artists_table.write.mode("overwrite").save(f'{output_data}artists_table.parquet')
    t2 = time.time()
    print(f"Writing artist_table took {(t2 - t1):.2f} seconds!")    

def process_log_data(spark, input_data, output_data):
    """
    Using spark context, read log data from specified folder 'input_data' and write fact/dimension tables to location 'output_data'.
    """
    # get filepath to log data file
    log_data_path = f"{input_data}/log_data"

    # read log data file
    log_data_schema = StructType([
        StructField("artist", StringType()),
        StructField("auth", StringType()),
        StructField("firstName", StringType()),
        StructField("gender", StringType()),
        StructField("itemInSession", LongType()),
        StructField("lastName", StringType()),
        StructField("length", DoubleType()),
        StructField("level", StringType()),
        StructField("location", StringType()),
        StructField("method", StringType()),
        StructField("page", StringType()),
        StructField("registration", DoubleType()),
        StructField("sessionId", LongType()),
        StructField("song", StringType()),
        StructField("status", LongType()),
        StructField("ts", LongType()),
        StructField("userAgent", StringType()),
        StructField("userId", StringType()),
    ])

    # and reload data
    log_data = spark.read\
        .schema(log_data_schema)\
        .option('mode', 'DROPMALFORMED')\
        .json(log_data_path)\
        .withColumn("ts", F.to_timestamp(F.col("ts") / 1000.)) # convert milliseconds to seconds and then convert to timestamp
    
    # filter by actions for song plays => this is done in the songplay table query

    # extract columns for users table    
    spark.sparkContext.setJobGroup("Select", "Selecting users_table")
    users_table = log_data.select(F.col("userId").alias("user_id"),\
                       F.col("firstName").alias("first_name"),
                       F.col("lastName").alias("last_name"),
                       F.col("gender").alias("gender"),
                       F.col("level").alias("level"))\
                    .drop_duplicates(subset = ['user_id'])
    
    # write users table to parquet files
    t1 = time.time()
    spark.sparkContext.setJobGroup("Write", "Writing users_table")
    users_table.write.mode("overwrite").save(f'{output_data}users_table.parquet')
    t2 = time.time()
    print(f"Writing users_table took {(t2 - t1):.2f} seconds!")

    # create timestamp column from original timestamp column & create datetime column from original timestamp column
    # both these conversions have been done in place when loading the data!
    
    # extract columns to create time table
    spark.sparkContext.setJobGroup("Select", "Selecting time_table")
    time_table = log_data.select(F.col("ts").alias("start_time"))\
        .distinct()\
        .withColumn("hour", F.hour("start_time"))\
        .withColumn("day", F.dayofweek("start_time"))\
        .withColumn("week", F.weekofyear("start_time"))\
        .withColumn("month", F.month("start_time"))\
        .withColumn("year", F.year("start_time"))\
        .withColumn("weekday", F.udf(lambda x: 0 if x in [7, 1] else 1, LongType())(F.col("day")))
    
    # write time table to parquet files partitioned by year and month
    t1 = time.time()
    spark.sparkContext.setJobGroup("Write", "Writing time_table")
    time_table.write.mode("overwrite").partitionBy(['year', 'month']).save(f'{output_data}time_table.parquet')
    t2 = time.time()
    print(f"Writing time_table took {(t2 - t1):.2f} seconds!")    

    # read in song data to use for songplays table
    song_data_schema = StructType([
        StructField("artist_id", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("artist_longitude", DoubleType()),
        StructField("artist_name", StringType()),
        StructField("duration", DoubleType()),
        StructField("num_songs", LongType()),
        StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("year", LongType()),
    ])

    # read with explicitly specified schema
#     song_data_path = f"{input_data}song_data/A/A/A/*.json"
    song_data_path = f"{input_data}song_data/*/*/*/*.json"
    song_data = spark.read \
        .schema(song_data_schema) \
        .option('mode', 'DROPMALFORMED') \
        .json(song_data_path)


    # extract columns from joined song and log datasets to create songplays table 
    spark.sparkContext.setJobGroup("Select", "Selecting songplays_table")
    songplays_table = log_data.join(song_data, \
              [log_data.artist == song_data.artist_name, log_data.song == song_data.title])\
            .filter(F.col('page') == 'NextSong')\
            .select(F.col("ts").alias("start_time"),\
                    F.col("userId").alias("user_id"),\
                    "level",\
                    "song_id",\
                    "artist_id",\
                    F.col("sessionId").alias("session_id"),\
                    "location",\
                    F.col("userAgent").alias("user_agent"),\
                    F.year("ts").alias("year"),\
                    F.month("ts").alias("month"))

    # write songplays table to parquet files partitioned by year and month
    t1 = time.time()
    spark.sparkContext.setJobGroup("Write", "Writing songplays_table")
    songplays_table.write.mode("overwrite").partitionBy(['year', 'month']).save(f'{output_data}songplays_table.parquet')
    t2 = time.time()
    print(f"Writing songplays_table took {(t2 - t1):.2f} seconds!")    


def main():
    """
    Driver function. Set up spark session, read from s3 bucket (specified by input_data) and write fact/dimension tables to another s3 bucket (specified by output_data).
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    # store data locally, replace with s3 url later
    # output_data = "./data/"
    output_data = "s3://jjudacitydatalake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
