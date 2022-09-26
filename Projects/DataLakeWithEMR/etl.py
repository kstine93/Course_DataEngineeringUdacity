import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


aws_path = "/home/rambino/.aws/credentials"
aws_cred = configparser.ConfigParser()
aws_cred.read(aws_path)

os.environ['AWS_ACCESS_KEY_ID'] = aws_cred['default']['aws_access_key_id']
os.environ['AWS_SECRET_ACCESS_KEY'] = aws_cred['default']['aws_secret_access_key']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = "s3a://more_filepath....."
    
    # read song data file
    df = spark.read.format().path()

    # extract columns to create songs table
    songs_table = df.select(col1, col2)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.format().path() #How to partition into different folders?

    # extract columns to create artists table
    artists_table = df.select(col1, col2)
    
    # write artists table to parquet files
    artists_table.write.format().path() #How to partition into different folders?


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data =

    # read log data file
    df = 
    
    # filter by actions for song plays
    df = 

    # extract columns for users table    
    artists_table = 
    
    # write users table to parquet files
    artists_table

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = 
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = 
    
    # extract columns to create time table
    time_table = 
    
    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    #Note: song data must be processed first, since its output is necessary for log data tables
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
