import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, from_unixtime, dayofweek

#parse config to get credentials
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS','AWS_SECRET_ACCESS_KEY')

#function to create a spark session
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """Function to read source song files from S3 and output 
       parquet files for songs and artists back on S3
    
    Args:
        spark: to read files with spark
        input_data: source location for S3 Bucket
        output_data: destination location for S3 Bucket
    
    Output Files:
        s3://output-datalakes/songs/songs.parquet
        s3://output-datalakes/artists/artists.parquet
    """
    # get filepath to song data file
    song_data = os.path.join(input_data,"song_data")
    
    # read song data file
    df = spark.read.json(song_data)
    
    # extract columns to create songs table
    songs_table = df.select('song_id', \
                            'title', \
                            'artist_id', \
                            'year',  \
                            'duration').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").parquet(os.path.join(output_data,"songs/","songs.parquet")) 

    # extract columns to create artists table
    artists_table = df.select('artist_id', \
                              'artist_name', \
                              'artist_location', \
                              'artist_latitude', \
                              'artist_longitude').dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data,"artists/","artists.parquet"))

def process_log_data(spark, input_data, output_data):
    """Function to read source log files from S3 and output 
       parquet files for users, time and songplays back on S3
    
    Args:
        spark: to read files with spark
        input_data: source location for S3 Bucket
        output_data: destination location for S3 Bucket
    
    Output Files:
        s3://output-datalakes/users/users.parquet
        s3://output-datalakes/time/time.parquet
        s3://output-datalakes/songplays/songplays.parquet
    """
    # get filepath to log data file
    log_data =os.path.join(input_data,"log_data/")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays and also create songplay_id incremental key
    df = df.filter(df['page'] == "NextSong").withColumn('songplay_id', monotonically_increasing_id())

    # extract columns for users table    
    users_table = df.select('userid', \
                            'firstName', \
                            'lastName', \
                            'gender', \
                            'level').dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data,"users/","users.parquet"))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int(x/1000))
    df = df.withColumn('start_time',from_unixtime(get_timestamp(df['ts'])))
    
    # extract columns to create time table
    time_table = df.select('start_time') \
                   .withColumn('hour',hour(df['start_time'])) \
                   .withColumn('day',dayofmonth(df['start_time'])) \
                   .withColumn('week',weekofyear(df['start_time'])) \
                   .withColumn('month',month(df['start_time'])) \
                   .withColumn('year',year(df['start_time'])) \
                   .withColumn('weekday',dayofweek(df['start_time'])) \
                   .dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").parquet(os.path.join(output_data,"time/","time.parquet"))

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data, "song_data/A/A/A"))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df,(df.song == song_df.title) & (df.artist == song_df.artist_name),'left_outer') \
                        .select(df.songplay_id, \
                                df.start_time, \
                                df.userId, \
                                df.level, \
                                song_df.song_id, \
                                song_df.artist_id, \
                                df.sessionId, \
                                df.location, \
                                df.userAgent) \
                         .dropDuplicates()
    songplays_table.show()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("start_time").parquet(os.path.join(output_data,"songplays/","songplays.parquet"))
    
def main():
    """Main function to run other python functions
    
    Args:
        N/A
    
    Output:
        1) Create spark connection
        2) Create input and output path variables
        2) Process song file
        3) Process log file
    """
    print ("creating spark session")
    spark = create_spark_session()
    print ("spark session created")
    
    input_data = "s3a://udacity-dend/"
    output_data = "s3://output-datalakes/"

    print("processing song file...")
    process_song_data(spark, input_data, output_data)
    print("End of song processing")
    
    print("processing log file...")
    process_log_data(spark, input_data, output_data)
    print("End of log processing")
    
if __name__ == "__main__":
    main()
