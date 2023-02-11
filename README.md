# Data Warehouse (S3 to AWS Redshift ETL)
This is an ETL (Extract, Transform, Load) data pipeline project, which leverages AWS services (S3 and Redshift) to build a data warehouse for a startup company.  Go [here](https://github.com/Hyacinth-Ali/data-warehouse-S3-to-Redshift-ETL/blob/master/README.md#implementation-steps-and-how-to-run-the-project) if you just want to run the project, and for step by step (with code) implementation of this project, visit this [page](https://github.com/Hyacinth-Ali/data-warehouse-S3-to-Redshift-ETL/blob/master/redshift_cluster.ipynb)

## Problem Statement
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

![AWS S3 to Redshift ETL ARchitecture](https://user-images.githubusercontent.com/24963911/217428418-7a836be4-809f-46db-8f51-6c33b92f37a0.png)

## Project Dataset
As shown in the image above, there are three datasets in S3 (Song, log, and log_json_path datasets), which are required to extract, transform, and then load into Redshift. 

### Song Dataset
The first dataset is a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are file paths to two files in this dataset.
```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```
And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.
```
{
  "num_songs": 1, 
  "artist_id": "ARJIE2Y1187B994AB7", 
  "artist_latitude": null, 
  "artist_longitude": null, 
  "artist_location": "", 
  "artist_name": "Line Renaud", 
  "song_id": "SOUPIRU12A6D4FA1E1", 
  "title": "Der Kleine Dompfaff", 
  "duration": 152.92036, 
  "year": 0
}
```
### Log Dataset
The second dataset consists of log files in JSON format generated by this [event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

The log files in the dataset are partitioned by year and month. For example, here are file paths to two files in this dataset.
```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```

And below is an example of what the data in a log file, 2018-11-12-events.json, looks like.

![Log Data Image](https://user-images.githubusercontent.com/24963911/217435030-6e93f449-4689-466a-8f9b-4478cf604292.png)

### Log_Json Dataset
The content of the third dataset, which contains meta information about the log dataset, is shown below. This file contains the meta information that is required by AWS to correctly load the lod dataset.

![Log Json Path](https://user-images.githubusercontent.com/24963911/217435680-a5f90d3c-b6ee-4ad2-82a2-c684572c7173.png)


## Implementation Steps and How to Run the Project
Clone the project and then follow the steps below at the root directory (data-warehouse-s3-to-redshift-etl) to run the project.
1. **Provision Computing Resources**: Create different AWS computing resources, with **Infrasctructure as Code** paradigm, that are required to implement this project, including the redshift cluster.
```
python provision_resources.py 
```
2. **Create Tables**: Create tables in the redshift cluster to contain the datasets from AWS S3, i.e., staging tables and final tables. The final tables are based on **Star Schema** approach, as depicted below.
![Final Tables Schema](https://user-images.githubusercontent.com/24963911/218272966-10eb8712-f653-4dd2-bfc8-6a1e214737e9.png)


**Run this scriot to create the tables**
```
python create_tables.py 
```
3. **Insert Data into the Tables**: Insert data into the tables by copying datasets from S3 to staging tables, and finally inserting datasets from the staging tables to the final tables.
```
python etl.py 
```
4. **Run Exploratory Analysis**: At this juncture, the datasets sit in redshift cluster ready to be explored to provide insights into the music startup company.
```
python explore.py 
```
5. **Tear Down the Resoources**: Delete the resources for this project on AWS
```
python teardown.py
```
For more details about the results of these steps, visit this [page](https://github.com/Hyacinth-Ali/data-warehouse-S3-to-Redshift-ETL/blob/master/redshift_cluster.ipynb) to see both the code and the corresponding output of each command.
