# Udacity Data Analyst Nanodegree <br>Project: Data Modeling with Postgres 
### By: Amanda Hanway, 12/5/2023 
  
## Project Overview:  

Sparkify is a (fictional) startup that operates a music streaming app. The app collects data on user activity, songplays, and song metadata in JSON logs. The analytics team cannot access the app data in its current form, causing work stoppage as they are unable to perform any analyses on the app. Sparkify has hired me as a data engineer and my first project is to create a relational database schema with an ETL pipeline, making the data accessible to the analytics team and facilitating their analyses. 

### Files: 

- [sql_queries.py](sql_queries.py)
  - Contains the drop table, create table, and insert statements.
- [create_tables.py](create_tables.py)
  - Calls the sql_queries.py file to create the tables.  
- [etl.py](etl.py)
  - Performs the ETL pipeline to populate the tables.

### JSON Data File Format:  

Song Data Example:  
- {"num_songs": 1, "artist_id": "ARD7TVE1187B99BFB1", "artist_latitude": null, "artist_longitude": null, "artist_location": "California - LA", "artist_name": "Casual", "song_id": "SOMZWCG12A8C13C480", "title": "I Didn't Mean To", "duration": 218.93179, "year": 0}  

Log Data Example:  
- {"artist":null, "auth":"Logged In", "firstName":"Peter", "gender":"M", "itemInSession":0, "lastName":"Griffin", "length":null, "level":"free", "location":"Quahog, Rhode Island", "method":"GET", "page":"Home", "registration":1540919166796.0, "sessionId":38, "song":null, "status":200, "ts":1541105830796, "userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"39"}  

### Database Schema Design

The database consists of one fact table and four dimension tables. The fact table contains the records in the log data associated with song plays, while the dimension tables contain descriptive attributes related to the users, songs, artists, and start time. Each table has a primary key which serves as a unique identifier for each record in the table, and is used to join the dimension tables to the fact table.

Together, the tables form a star schema. The star schema design provides efficient storage by reducing redundant data being stored in multiple places, and will be simple for the analytics team to understand and utilize for quick access to the data. 

Fact Table:

- songplays   
    - Description: records in log data associated with song plays, i.e. records with page NextSong
    - Columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables:

- users  
    - Description: users in the app
    - Columns: user_id, first_name, last_name, gender, level

- songs  
    - Description: songs in music database
    - Columns: song_id, title, artist_id, year, duration

- artists  
    - Description: artists in music database
    - Columns: artist_id, artist_name, artist_location, artist_latitude, artist_longitude

- time  
    - Description: timestamps of records in songplays broken down into specific units   
    - Columns: start_time, hour, day, week, month, year, weekday

![Diagram](images/star_schema.png?raw=true "Diagram")

### ETL Pipeline

The ETL Pipeline automates the extracting, transforming, and loading of the JSON files into the PostgreSQL database by iterating through all data files in the directory. 

The pipeline is executed from the terminal as shown and described below.  

- Once the tables have been created, the [etl.py](etl.py) file is executed to perform the ETL pipeline.  
    - It first identifies the total count of files in the directory, then processes each file individually.  
    - The general steps performed on each file are as follows:  
        1. Open the file  
        2. Read the contents into a dataframe  
        3. Perform transformations, including filtering, converting datatypes, and creating new columns  
        4. Extract the columns relevant for the table  
        5. Insert the data into the table 
        6. Iterate for remaining files  
    
![Processing Song Data Files](images/terminal_running_song_data.png?raw=true "song data")  ![Processing Log Data Files](images/terminal_running_log_data.png?raw=true "log data") 

### Example Queries & Analysis

**Total count of songplays by level:**  
```sql
select 
    level
    , count(distinct user_id) as user_count
    , count(distinct songplay_id) as song_play_count
from songplays 
group by level;
```
There are fewer users on the paid level, but they consume four times as many songs as those on the free level.

|level | user_count | song_play_count |
| ---- | ---------- | --------------- |
|free  | 88         | 1229            |
|paid  | 22         | 5591            |  

   
**Total count of song plays by user:**  
```sql
select 
    user_id
    , count(distinct songplay_id) as play_count 
from songplays 
group by user_id 
order by count(distinct songplay_id) desc;
```
The top five users by song plays are user_id's 49, 80, 97, 15, and 44.

|    | user_id | play_count |
| -- | ------- | ---------- |
| 1. | 49      | 689        |
| 2. | 80      | 665        |
| 3. | 97      | 557        |
| 4. | 15      | 463        |
| 5. | 44      | 397        |


















