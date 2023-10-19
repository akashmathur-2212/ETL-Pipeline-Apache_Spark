# Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

# Project Datasets
We will be working with two datasets that reside in S3. Here are the S3 links for each:

- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data

# Goal
- Build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables.

# How to run ?
- First we need to AWS access key id and AWS secret key for the IAM user whose information is present in dl.cfg. This is later parsed in etl.py.
```AWS_ACCESS_KEY_ID= '' AWS_SECRET_ACCESS_KEY= ''```

- We also need to specify a new S3 bucket with both read and write roles (full access).

- Finally, we will run etl.py in the terminal.

# Project Structure
- dl.cfg : Contains AWS access key and secret access key.

- etly.py: Read data from public S3 bucket and using the power of Spark performed data wrangling and data transformation to create dimension and fact tables in parquet format which is further pushed into the S3 bucket created for the IAM user.

- README.md: For the detailed documentaion of the ETL development.

- Testing.ipynb: Tested codes before implementing inside the automated etl.py file.

# ETL Pipeline:
1. Parsed the dl.cfg to get access of the S3 bucket.

2. Created a spark session ; create_spark_session() .

3. Then, process_song_data() will perform transformation of song data.

- This function will create two dimensions tables namely song and artist.

- Finally, the dimension table will be written to the S3 bucket in parquet format which saves space and makes reading more efficient.