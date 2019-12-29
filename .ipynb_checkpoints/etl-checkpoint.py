import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col,unix_timestamp
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import DateType, TimestampType, IntegerType;

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get("KEYS", "AWS_ACCESS_KEY_ID");
os.environ['AWS_SECRET_ACCESS_KEY']=config.get("KEYS", "AWS_SECRET_ACCESS_KEY");


def create_spark_session():
    """
     Creates the spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.3") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process the song data by reading from s3 based on input_data, and write to s3 based on output_data

    Parameters
    ----------
    spark : SparkSession
        Spark Session 
    input_data : str
        Location of input files in s3
    output_data : str
        Location of output files in s3
    """
    # get filepath to song data file
    song_data = input_data + "/song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.option("inferschema", "true").json(song_data);

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").parquet(output_data + "song-data.parquet", mode="overwrite")

    # extract columns to create artists table
    artists_table = df.select("artist_id", col("artist_name").alias("name"), col("artist_location").alias("location"), col("artist_latitude").alias("latitude"), col("artist_longitude").alias("longitude"));
    
    # write artists table to parquet files
    artists_table = artists_table.write.parquet(output_data + "/data/artists-data.parquet", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    Process the log data by reading from s3 based on input_data, and write to s3 based on output_data

    Parameters
    ----------
    spark : SparkSession
        Spark Session 
    input_data : str
        Location of input files in s3
    output_data : str
        Location of output files in s3
    """
    # get filepath to log data file
    log_data = input_data + "/log_data/*/*/*.json"

    # read log data file
    df = spark.read.option("inferschema", "true").json(log_data);
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(col("userId").alias("user_id"), col("firstName").alias("first_name"), col("lastName").alias("last_name"), "gender", "level");

    # write users table to parquet files
    users_table = users_table.write.parquet(output_data + "/users-data.parquet", mode="overwrite")
    get_timestamp = udf(lambda x: datetime.utcfromtimestamp(x / 1000.0), DateType())
    df = df.withColumn("timestamp_datetime", get_timestamp("ts"))

    get_weekday = udf(lambda x: x.weekday(), IntegerType()) 

    time_table = df.select(col("timestamp_datetime").alias("start_time"), hour("timestamp_datetime").alias("hour"), dayofmonth("timestamp_datetime").alias("day"), weekofyear("timestamp_datetime").alias("week"), month("timestamp_datetime").alias("month"), year("timestamp_datetime").alias("year"), get_weekday("timestamp_datetime").alias("weekday"));
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + "/time-data.parquet", mode="overwrite")

    # read in song data to use for songplays table
    song_data = input_data + "/song_data/*/*/*/*.json"
    song_df = spark.read.option("inferschema", "true").json(song_data);
    df = df.filter(df.page == "NextSong").filter(df.userId.isNotNull())
    joined_df = df.join(song_df, (df.artist == song_df.artist_name) & (df.song == song_df.title))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = joined_df.select(year("timestamp_datetime").alias("year"), month("timestamp_datetime").alias("month"), col("timestamp_datetime").alias("start_time"), col("userId").alias("user_id"), "level", "song_id", "artist_id", col("sessionId").alias("session_id"), col("location").alias("location_id"), col("userAgent").alias("user_agent"))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data + "/songplays-data.parquet", mode="overwrite")


def main():
    """
    The main point of entry.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-youliang/"
    output_data = "s3a://udacity-youliang/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
