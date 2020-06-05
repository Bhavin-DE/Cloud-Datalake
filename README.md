# Overview:

Sparkify is a music streaming app used by users to listen songs. Sparkify is collecting data about user activity and songs through their app on AWS S3. Now they want to use this collected data to create a data warehouse on Redshift which will help them analyse songs and user activity and in turn provide better user experience.

# Source Files:

1) Song Files - which holds information about song metadata along with artist details
2) Log Files - which holds details about user activity 

# Requirements:

Use spark to process source files from S3 and then create parquet files as output back on S3 through EMR Cluster.


# Specifications:

- Create EMR Cluster
- Create S3 output folders
- Process source files (songs and logs files) using Spark
- Create songs, artists, users, time and song plays tables
- Output tables into parquet files back on S3

## Source files:

- s3a://udacity-dend/song_data/
 - Fields: num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year

- s3a://udacity-dend/log_data/
 - Fields: artist, auth, firstName, gender, iteminSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userid

## Target files:

- s3://output-datalakes/songs/songs.parquet
 - Fields: song_id, title, artist_id, year, duration

- s3://output-datalakes/artists/artists.parquet
 - Fields: artist_id, artist_name, artist_location, artist_latitude, artist_longitude

- s3://output-datalakes/users/users.parquet
 - Fields: userid, firstName, lastName, gender, level

- s3://output-datalakes/time/time.parquet
 - Fields: start_time, hour, day, week, month, year, weekday

- s3://output-datalakes/songplays/songplays.parquet
 - Fields: songplay_id, start_time, userId, level, song_id, artist_id, sessionId, location, userAgent

# Scripts:

1) dl.cfg - This file has access key which is then used in eta.py script. No need to run this.
2) etl.py - This is the script which will process source files from S3, performs data manipulation, creates 5 tables and then outputs 5 tables back to S3 in parquet format.