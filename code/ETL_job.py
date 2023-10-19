import configparser
from datetime import datetime
import os
import calendar
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as F
from pyspark.sql import types as T
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/A/A/A/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', col('title').alias('song_title'), 'artist_id', 'year', 'duration').distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data+ "song.parquet", mode="overwrite")

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', col('artist_latitude').alias('latitude'), col('artist_longitude').alias('longitude')).distinct()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "aritist.parquet", mode = "overwrite")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data/*/*"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    user_table = df.select(col('userId').alias('user_id'), 'gender', 'level','firstName', 'lastName').distinct()
    
    # write users table to parquet files
    user_table.write.parquet(output_data + "users.parquet", mode="overwrite")

    # create timestamp column from original timestamp column
    df = df.withColumn(
        "ts_timestamp",
        F.to_timestamp(F.from_unixtime((col("ts") / 1000) , 'yyyy-MM-dd HH:mm:ss.SSS')).cast("Timestamp")
    )
    
    #get weekday

    def get_weekday(date):
        date = date.strftime("%m-%d-%Y")
        month, day, year = (int(x) for x in date.split('-'))
        weekday = datetime.date(year, month, day)
        return calendar.day_name[weekday.weekday()]

    udf_week_day = udf(get_weekday, T.StringType()) 
                              

    # extract columns to create time table

    time_table = df.withColumn("hour", hour(col("ts_timestamp"))).withColumn("day", hour(col("ts_timestamp"))).withColumn("week", weekofyear(col("ts_timestamp")))\
            .withColumn("weekday", udf_week_day(col("ts_timestamp")))\
            .withColumn("month", month(col("ts_timestamp")))\
            .withColumn("year", year(col("ts_timestamp")))\
            .select(col("ts_timestamp").alias("time"),"hour", "day", "week", "weekday", "month", "year" )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + "time.parquet", mode="overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "song.parquet")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.withColumn("Songplay_Id", F.monotonically_increasing_id())\
                .join(song_df, song_df.song_title == df.song)\
                .select('Songplay_Id', col('userId').alias('user_id'), 'song_id', 'artist_id',\
                col('sessionId').alias('session_id'),'level',"location", "userAgent"\
                ,col("ts_timestamp").alias("start_time"))
                        

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + "songplay.parquet", mode="overwrite")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://jubin-sparkify-datalake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
