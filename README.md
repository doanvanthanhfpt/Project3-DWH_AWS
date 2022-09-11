# About Data Analysis Project
Sparkify, a music streaming startup, has move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This documentation guides building an ETL pipeline that extracts Sparkify's data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for analytics steps. This guides the way to test database and ETL pipeline by running queries given by the analytics team from Sparkify and compare the results with expected results.

## About data sets
Two datasets that reside in S3. Here are the S3 links for each:
- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data

Log data json path: s3://udacity-dend/log_json_path.json

## Design database schema
Database schema includes the following fact and dimension tables:

1. Fact Table
    - songplays: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

2. Dimension Tables
    - users: user_id, first_name, last_name, gender, level
    - songs: song_id, title, artist_id, year, duration
    - artists: artist_id, name, location, lattitude, longitude
    - time: start_time, hour, day, week, month, year, weekday

3. Staging Tables to get data from log events and song events
    - staging_events: artist, auth , firstName , gender , itemInSession, lastName , length, level , location , method , page , registration, sessionId, song , status, ts, userAgent , userId
    - staging_songs: num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year


## Prerequisites
- Chrome browser Version 105.0.5195.102 (Official Build) or newer.
- Jupyter launching from Anacoda3 installed.
- Python 3.10 with corresponding modules configparser, psycopg2, pandas, boto3, json installed on local machine.
- Visual Studio Code Version 1.71.0 or newer with python, jupyter extensions installed.
- AWS root account (or another account with the same privilege).
- Git version 2.25.1

## Step 0: Clone from Github repo :
- Go to specific folder, right click on folder space, click ***Git Bash Here*** to open commandline GUI.
- Run commandline: ***git clone https://github.com/doanvanthanhfpt/Project3-DWH_AWS***

Note: keep command GUI for next steps.

## Step 1: Create and update Redshift administrator account to config file
- Go to AWS IAM service https://us-east-1.console.aws.amazon.com/iamv2/home?region=us-east-1#/users and click on the "***Add user***" button to create a new IAM user in your AWS account.
- Choose a name of your choice.
- Select "***Programmatic access***" as the access type. Click Next.
- Choose the ***Attach existing policies directly*** tab, and select the "***AdministratorAccess***". Click ***Next***.
- Skip adding any tags. Click ***Next***.
- Review and create the user. It will show you a pair of access key ID and secret.
- Take note of the pair of access key ID and secret. This pair is collectively known as Access key.
- Edit the file ***dwh.cfg*** in the same folder, save the access key and secret against the following variables and then save it:
                KEY= <YOUR_AWS_KEY>
                SECRET= <YOUR_AWS_SECRET>

## Step 2: Create Redshift cluster with Infrastructure as Code (IaC) script
From command GUI, run ***aws_redshift-IaC.ipynb*** Jupyter notebook step by step to check the outputs.

Importance note: 
    - When redshift-cluster display status ***Available***, update config file ***HOST*** value with ***DWH_ENDPOINT*** value.
    - Stop running IaC after **Step 4: Verify cluster connected**

## Step 3: Create database schema, tables and get data from S3 to Redshift DB
From command GUI, run ***create_tables.py*** with command:
    >***python create_tables.py***
 
## Step 4: Test by  running the analytic queries on Redshift database
From command GUI, run ***etl.py*** with command:
    >***python etl.py***

## Example queries and results for song analysis
- Count number of staging tables: 
    Result of ***staging_events*** should be 8056
    Result of ***staging_songs*** should be 14896

    SELECT COUNT(*) FROM staging_events;
    SELECT COUNT(*) FROM staging_songs;

- Count number of fact table records. Result should be 319
SELECT COUNT(*) FROM songplay;

- Count number of dimension table records of ***users***. Result should be 6820
SELECT COUNT(*) FROM users;

- Count number of ***songplay_id*** NULL. Result should be 0
SELECT COUNT(*) FROM songplay
WHERE songplay_id = NULL;

## Delete Redshift cluster to aviod unexpectation fee
Run ***aws_redshift-IaC.ipynb*** Jupyter notebook from **### Step 5: Clean up resource** to the end of IaC.

## End of documentations