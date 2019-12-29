## How to run the ETL process.
1) Setup your aws access keys at dl.cfg
2) Run the following commands

```
import etl as e;
e.main();
```

## Purpose of this dataset in the context of the startup, Sparkify, and their analytical goals.

 - This dataset contains analytical information about the usage of Sparkify users song play represented as a fact table using star schema. It also contains dimension data of user information, songs, artists and timestamps of records in songplay.
 - Sparkify oftens want to know where their user is located at, and what songs interest them. So that they can procure better song content with music provider.
 - Sparkify also wants to popular song and artist in the area so that they can do song recommendation to their user in similar location.

## Database schema design and ETL pipeline.
 - The data is modelled using star schema. Song play data is represented as a fact table and user information, songs, artists and timestamps of records in songplay is represented as dimension table.
 - The ETL pipeline consists of s3 and spark. The first step is for spark to read from s3 to perform transformation. The next step is to save the fact and dimension table into actual star schema tables in s3.

## Directory

 - data/ : Contains sample data files
 - dl.cfg : Configuration for aws access keys
 - etl.py: etl main file. You can run main() method here to start the etl process
 - README.md: This file that you are reading.