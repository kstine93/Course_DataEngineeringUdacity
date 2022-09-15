from pyspark.sql import SparkSession
#import configparser

def write_to_s3():
    '''This function written mostly by Udacity
    Sole purpose is to process given 'cities.csv' from S3,
    then write results to another S3 bucket'''

    #Separate config file shows paths of files to process + where to output
    #paths = configparser.ConfigParser()
    #paths.read("s3://emr-input-output/config/testPaths.cfg")
    input_path = "s3a://emr-input-output/input/cities.csv" #paths['PATHS']['input']
    output_path = "s3a://emr-input-output/output/" #paths['PATHS']['output']

    spark = SparkSession \
        .builder \
        .appName("write file to S3") \
        .getOrCreate()

    df = spark.read.format("csv").option("header","true").load(input_path)

    # investigate what columns you have
    col_list = df.columns
    print(col_list)
    agg_df = df.groupby("country").count()

    agg_df.write.format("parquet").mode("overwrite").save(output_path + "output.parquet")

    spark.stop()

if __name__ == "__main__":
	write_to_s3()