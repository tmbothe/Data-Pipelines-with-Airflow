# Data-Pipelines-with-Airflow
In this project, we will extract, transform and load data from S3 to Redshift using Apache  Airflow as orchestration tool.

## Project Description

A compagny that has been collecting songs' data and users activity wants an efficient model to store and analyze the data. The data currently resides in AWS S3 bucket in JSON format.
This project consists of building an ETL pipeline that extracts the data from S3, processing using apache Airflow, and loads back in a datawarehouse houses in AWS redshift.
This will help the analytics to analyze and derive insights from data.

## Data description
There are two sources of dataset, the **Song Dataset** and the **Log Dataset** .  Both dataset are currently stored in amazon S3. These files will be read from Apache Airflow and stored in some staging tables. Then another process will extract the data from staging tables and load in final tables in AWS redshift datawarehouse.

### The Song Dataset
The song data is stored at : `s3://udacity-dend/song_data`  
This is a collection of JSON data that store songs data. The files are partitioned by the three letters of of each song's track ID. These are two examples:

 - **song_data/A/B/C/TRABCEI128F424C983.json**
 - **song_data/A/A/B/TRAABJL12903CDCF1A.json**
 
 Each file is structured as:
 
 {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

### Log Dataset

The Log dataset is stored at `Log data: s3://udacity-dend/log_data` and the JSON path is `s3://udacity-dend/log_data`.

The second dataset is a set of JSON log files generated by an event similator base of songs dataset above. These files simulate activity logs partitioned by year and month as we can see below:

- **log_data/2018/11/2018-11-12-events.json**
- **log_data/2018/11/2018-11-13-events.json**

The file is structured as :
![image](https://raw.githubusercontent.com/tmbothe/Data_Warehouse_Project/main/images/log-data.png)


## Choice of Data Model

For this project, we will building a star model with fact and dimension tables. This schema is analytic focus as all dimensions tables are one join away from that fact tables that easier queries retrieval. Here is tables description:

### statging tables structure
 The staging tables will store all fields as they are from the files in S3
 - **staging events table** 
 'staging_events
    artist    
    auth      
    firstName 
    gender    
    itemInSession 
    lastName  
    length    
    level 
    location
    method 
    page 
    registration 
    sessionId 
    song 
    status igint,
    ts  
    userAgent 
    userId  
 '
 - **staging songs tables**
 ` staging_songs
     num_songs 
     artist_id
     artist_latitude 
     artist_longitude
     artist_location 
     artist_name 
     song_id 
     title 
     duration
     year
     `
### Final tables

**Fact Table**

    1 - songplays - records in log data associated with song plays i.e. records with page NextSong
         songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
         
**Dimension Tables**

    2 - users - users in the app
         user_id, first_name, last_name, gender, level 
    3 - songs - songs in music database
         song_id, title, artist_id, year, duration
    4- artists - artists in music database
        artist_id, name, location, latitude, longitude
    5- time - timestamps of records in songplays broken down into specific units
        start_time, hour, day, week, month, year, weekday
 ### Star Schema Design       
 ![image](https://raw.githubusercontent.com/tmbothe/Data_Warehouse_Project/main/images/datamodel.PNG)
 
 ## Project Structure
 ```
 The project has two main files, here is the description:
   Data-Pipelines-with-Airflow
    |
    |   dags
    |      | sql_statements.py
    |      | udac_airflow_dag.py
    |      | images 
    |   plugins
    |      | helpers
    |          sql_queries.py
    |   operators
    |      | data_quality.py
    |      | load_dimension.py
    |      | load_fact.py
    |      | stage_redshift.py
 ``` 

   1 - `sql_statements.py` : Under the dag folder, the sql_statements has scripts to create staging and datawarehouse tables.<br>
   2 - `udac_airflow_dag`  : the udac_airflow_dag file contains all airflow task and DAG definition.<br>
 
   **The  plugins folder has two subfolder: the helpers folders that contains the helpers files, and the operators folder that has all customs operators define for the project. <br>**
   3 - `sql_queries.py`    : This file has all select statements to populate all facts and dimension tables.<br>
   4 - `data_quality.py`   : This file defines all customs logic that will help checking the data quality once the ETL is complete.<br>
   5 - `load_dimension.py` : File to load dimension tables.<br>
   6 - `load_fact.py`      : File to load fact table.<br>
   7 -  `stage_redshift.py`:  File to load staging tables.<br>
 
## Installation 

- Install [python 3.8](https://www.python.org)
- Install [Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/installation.html)
- Clone the current repository. 
- Create IAM user in AWS and get the user access key and secret key.
- Launch and AWS redshift cluster and get the endpoint url as well as database connection information (Database name, port number , username and password).
- Follow the instruction below to configure Redshift as well as AWS credentials connections.
 ![image](https://raw.githubusercontent.com/tmbothe/Data-Pipelines-with-Airflow/main/dags/images/connections1.PNG)
 ![image](https://raw.githubusercontent.com/tmbothe/Data-Pipelines-with-Airflow/main/dags/images/connections2.PNG)
 ![image](https://raw.githubusercontent.com/tmbothe/Data-Pipelines-with-Airflow/main/dags/images/connections3.PNG)


 ## Final DAG

![image](https://raw.githubusercontent.com/tmbothe/Data-Pipelines-with-Airflow/main/dags/images/final_DAG.PNG)


 
