# Rust Cheaters Data Pipeline
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

<img src="https://github.com/jacob1421/RustCheatersDataPipeline/blob/master/images/rust.jpg" style="width: 100%;height:400px;" align="centre">

## Architecture 
![Pipeline Architecture](https://github.com/jacob1421/RustCheatersDataPipeline/blob/master/images/DataPipeline.jpg)

Pipeline Breakdown:
 - ETL Job
 - [Twitter API Python Library](https://github.com/tweepy/tweepy)
 - [Steam API](https://wiki.teamfortress.com/wiki/WebAPI)
 - Postgres Data Warehouse
 - Data Dashboard

### Overview
Rust cheater profiles are collected every hour from [@rusthackreport](https://twitter.com/rusthackreport) with the use of a [Airflow Python Operator](https://github.com/jacob1421/RustCheatersDataPipeline/blob/master/dags/scripts/helpers.py) and [Twitter API Python Library](https://github.com/tweepy/tweepy)
Cheater Steam profiles are collected from Steam using the [Steam Web API](https://wiki.teamfortress.com/wiki/WebAPI) with the use of a [Custom Airflow Operator](https://github.com/jacob1421/RustCheatersDataPipeline/blob/master/dags/custom_operators/SteamToS3Operator.py)
Data is collected and stored in a raw S3 bucket.
Raw S3 Bucket data is then transformed and stored in a staging bucket on S3.
Lastly, staging S3 bucket dim and fact data is loaded with Custom Airflow Operators [LoadDimOperator](https://github.com/jacob1421/RustCheatersDataPipeline/blob/master/dags/custom_operators/LoadDimsOperator.py) and [LoadFactOperator](https://github.com/jacob1421/RustCheatersDataPipeline/blob/master/dags/custom_operators/LoadFactsOperator.py)

### ETL Flow - Hourly
![ETL Arcitecture](https://github.com/jacob1421/RustCheatersDataPipeline/blob/master/images/airflow_dag.png)

 - Data collected from the Twitter API is moved to raw s3 bucket.
 - Twitter data is read from raw s3 bucket then profile urls are extracted and stored in a temp s3. 
 - Steam data collected from Steam Web API is moved to a raw s3 bucket.
 - Raw S3 Steam data undergoes transformations and data checks then stored in a staging s3 bucket. 
 - Data is transferred from staging S3 buckets into temp tables then into the data warehouse.
 - Dashboard can be used to gain insights about cheaters with the [Data Studio Dashboard](https://datastudio.google.com/u/0/reporting/85aa118b-9def-48e4-8c88-b3db1e34e3ff/page/Ic8kC).

## Data Warehouse
![Data Warehouse Arcitecture](https://github.com/jacob1421/RustCheatersDataPipeline/blob/master/images/DataWarehouse.png)

## [Data Studio Dashboard](https://datastudio.google.com/u/0/reporting/85aa118b-9def-48e4-8c88-b3db1e34e3ff/page/Ic8kC)
![Data Studio Dashboard](https://github.com/jacob1421/RustCheatersDataPipeline/blob/master/images/Dashboard.gif)

## Data Insights 
1. The US has the most accounts banned for cheating with Russia trailing behind.

2. Most cheaters have a level 1 steam account.

3. The top 3 cheater names

    -123

    -NeOn

    -xd

4. The most common profile picture is the default steam profile picture.

5. The majority of cheaters get banned between 0 and 10 hours.

6. The top 3 games that cheaters own

   - Counter-Strike: Global Offensive

   - PUBG: BATTLEGROUNDS

   - Apex Legends.

7. Top 3 Steam Groups

   - Rustoria

   - Andysolam

   - Payday

8. Cheaters use Archi's SC Farm to boost their accounts. It's a cheater's attempt to make their account look more legitimate to normal players.

9. Profile Visibility - A lot of people believe if a profile is private it's a cheater. More cheaters have public profiles than private profiles.

   - Friends of Friends - 2,565

   - Private - 824

   - Friends Only - 133
  

## Reflect
1.) Why not uses Spark?
The data that is processed every hour is between 1-5MB.

2.) Why stage the Fact and Dim tables pre load?
Easier to debug the pipeline in event that the pipeline fails.

## Acknowledgment
Emily(mod#1073) from [Data Engineering Discord](https://invite.gg/dataengineering)
   -Answered questions I had about my initial data warehouse architecture. Emily was very helpful in my adventure to building a data warehouse!
