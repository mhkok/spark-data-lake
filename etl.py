import pyspark
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This function creates as spark session to connect to spark
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function processes the song data. The following paramaters are used as input:
    - spark: the sprak session created in the function 'create_spark_session'
    - input_data: the song data location defined in the main function
    - output_data: the location of the output data defined in the main function
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/A/A/A/*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').distinct()

    # write songs table to parquet files partitioned by year and artist
    songs_table = songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data, "songs_tbl.parquet"), "overwrite")

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_longitude", "artist_latitude").distinct()
    
    # write artists table to parquet files
    artists_table = artists_table.write.parquet(os.path.join(output_data, "artists_tbl.parquet"), "overwrite")


def process_log_data(spark, input_data, output_data):
    """
    This function processes the log data. The following paramaters are used as input:
    - spark: the sprak session created in the function 'create_spark_session'
    - input_data: the log data location defined in the main function
    - output_data: the location of the output data defined in the main function
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/2018/*/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select("userId", "firstName", "lastName", "gender", "level").distinct()
    
    # write users table to parquet files
    users_table = users_table.write.parquet(os.path.join(output_data, 'users_tbl.parquet'), "overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%H:%M:%S'))
    df = df.withColumn('time_stamp', get_timestamp(df.ts))  
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
    df = df.withColumn("date_time", get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select("time_stamp", "date_time"). \
                withColumn("hour", hour(df.time_stamp)). \
                withColumn("day", dayofmonth(df.date_time)). \
                withColumn("week", weekofyear(df.date_time)). \
                withColumn("month", month(df.date_time)). \
                withColumn("year", year(df.date_time)). \
                withColumn("weekday", dayofweek(df.date_time)). \
                distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table = time_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "time_tbl.parquet"), "overwrite")

    # read in song data to use for songplays table
    song_df = os.path.join(input_data, 'song_data/A/A/A/*.json')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.artist == song_df.artist_name)) \
                    .select(col("time_stamp"), \
                        col("userId"), \
                        col("level"), \
                        col("song_id"), \
                        col("artist_id"), \
                        col("sessionId"), \
                        col("artist_location"), \
                        col("userAgent"), \
                        year("date_time").alias("year"), \
                        month("date_time").alias("month")) \
                    .withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "songplays_tbl.parquet"), "overwrite")


def main():
    """
    The main function that calls the different functions in the etl.py script. 
    This functiion creates a spark session, processes the song data & finally processes the log data. 
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://emr-output-data"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
