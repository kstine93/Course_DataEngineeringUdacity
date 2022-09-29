import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, expr
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, desc
from pyspark.sql.types import IntegerType, StringType, FloatType, StructType, StructField


aws_path = "/home/rambino/.aws/credentials"
aws_cred = configparser.ConfigParser()
aws_cred.read(aws_path)

#FOR TESTING
os.environ['AWS_ACCESS_KEY_ID'] = aws_cred['default']['aws_access_key_id']
os.environ['AWS_SECRET_ACCESS_KEY'] = aws_cred['default']['aws_secret_access_key']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages","com.amazonaws:aws-java-sdk-s3:1.12.311") \
        .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:3.3.4") \
        .getOrCreate()

    return spark

def get_log_schema():

        return StructType([
        StructField('artist',StringType(),True),
        StructField('auth',StringType(),True),
        StructField('firstName',StringType(),True),
        StructField('gender',StringType(),True),
        StructField('itemInSession',IntegerType(),True),
        StructField('lastName',StringType(),True),
        StructField('length',IntegerType(),True),
        StructField('level',StringType(),True),
        StructField('location',StringType(),True),
        StructField('method',StringType(),True),
        StructField('page',StringType(),True),
        StructField('registration',StringType(),True),
        StructField('sessionId',IntegerType(),True),
        StructField('song',StringType(),True),
        StructField('status',IntegerType(),True),
        StructField('ts',FloatType(),True),
        StructField('userAgent',StringType(),True),
        StructField('userId',StringType(),True)
    ])

def get_song_schema():

        return StructType([
        StructField('num_songs',IntegerType(),True),
        StructField('artist_id',StringType(),True),
        StructField('artist_latitude',FloatType(),True),
        StructField('artist_longitude',FloatType(),True),
        StructField('artist_location',StringType(),True),
        StructField('artist_name',StringType(),True),
        StructField('song_id',StringType(),True),
        StructField('title',StringType(),True),
        StructField('duration',FloatType(),True),
        StructField('year',IntegerType(),True)
    ])

def process_songplays(spark, song_df, log_df, output_path_prefix):
    # extract columns from joined song and log datasets to create songplays table 
    match_condition = [log_df.song == song_df.title, log_df.artist == song_df.artist_name]

    songplays_df = log_df.join(song_df, match_condition, "left") \
        .select(
            log_df.ts, log_df.userId, log_df.level, log_df.sessionId,
            log_df.location, log_df.userAgent, log_df.year, log_df.month,
            song_df.song_id, song_df.artist_id
        )

    songplays_df = songplays_df.withColumn('songplay_id',expr("uuid()"))

    songplays_df \
        .select("songplay_id","ts","userId","level","song_id","artist_id","sessionId","location","userAgent", "year","month") \
        .write \
        .option("header",True) \
        .partitionBy("year","month") \
        .csv(output_path_prefix + "songplays")   


def process_song_data(spark, song_schema, input_paths, output_path_prefix):

    # read song data file
    song_df = spark \
        .read \
        .format('json') \
        .schema(song_schema) \
        .load(input_paths['songs_raw'])

    # extract columns to create songs table
    songs_table = song_df.select('song_id','title','artist_id','year','duration')
    
    # write songs table to csv files partitioned by year and artist
    songs_table.write \
    .option("header",True) \
    .partitionBy("year","artist_id") \
    .csv(output_path_prefix + "songs")

    # extract columns to create artists table
    artists_table = song_df.select('artist_id','artist_name','artist_location','artist_latitude','artist_longitude')

    # write artists table to csv files
    artists_table.write \
    .option("header",True) \
    .partitionBy("artist_id") \
    .csv(output_path_prefix + "artists")

    return song_df


def process_log_data(spark, log_schema, input_paths, output_path_prefix):

    # read log data file
    log_df = spark \
        .read \
        .format('json') \
        .schema(log_schema) \
        .load(input_paths['logs_raw'])

    # filter by actions for song plays
    #log_df = df.filter() #Test in notebook

    # extract columns for users table - filtering out duplicates
    users_table = log_df.select('userId','firstName','lastName','gender','level')

    # filter out duplicate users - preferring more recent entries
    users_table = users_table \
        .orderBy(desc('ts')) \
        .dropDuplicates(['userId'])
    
    # write users table to csv files
    #QUESTION: SHOULD I PARTITION BY ANYTHING HERE? ALL HAVE HIGH CARDINALITY
    users_table.write \
    .option("header",True) \
    .csv(output_path_prefix + "users")

    #NOTE: I don't know why I would need this function.. not necessary for getting month, etc.
    # create timestamp column from original timestamp column
    #get_timestamp = udf()
    #df_time = log_df.withColumn('timestamp',get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000))
    log_df = log_df.withColumn('timestamp',get_datetime('ts'))

    # more udfs to extract specific parts of date from datetime
    get_hour = udf(lambda x: x.hour)
    get_day = udf(lambda x: x.day)
    get_week = udf(lambda x: x.isocalendar().week)
    get_month = udf(lambda x: x.month)
    get_year = udf(lambda x: x.year)
    get_weekday = udf(lambda x: x.weekday())
    
    log_df = log_df \
        .withColumn('hour',get_hour('timestamp')) \
        .withColumn('day',get_day('timestamp')) \
        .withColumn('week',get_week('timestamp')) \
        .withColumn('month',get_month('timestamp')) \
        .withColumn('year',get_year('timestamp')) \
        .withColumn('weekday',get_weekday('timestamp'))

    # extract columns to create time table
    time_table = log_df.select('ts','hour','day','week','month','year','weekday')
    
    # write time table to csv files partitioned by year and month
    time_table.write \
        .option("header",True) \
        .partitionBy("year","month") \
        .csv(output_path_prefix + "time")

    return log_df


def main():
    spark = create_spark_session()

    output_path_prefix = "./_out/"#"s3a://rambino-output/"

    input_paths = {
        "songs_raw":"s3a://udacity-dend/song_data/A/B",
        "logs_raw":"s3a://udacity-dend/log_data/2018/11/2018-11-13-events.json"
    }
    
    #Note: song data must be processed first, since its output is necessary for log data tables
    song_df = process_song_data(spark, get_song_schema(), input_paths, output_path_prefix)    
    log_df = process_log_data(spark, get_log_schema(), input_paths, output_path_prefix)
    process_songplays(spark, song_df, log_df, output_path_prefix)


if __name__ == "__main__":
    main()
