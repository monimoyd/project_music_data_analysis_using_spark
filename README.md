# project_music_data_analysis_using_spark
## Project on Music Data Analysis Using Spark

## Music Data Analysis

A leading music-catering company is planning to analyse large amount of data received from varieties of sources, namely mobile app and website to track the behaviour of users, classify users, calculate royalties associated with the song and make appropriate business strategies. The file server receives data files periodically after every 3 hours.

## Challenges and Optimisations:
- LookUp tables are in NoSQL databases. Integrate them with the actual data flow.
- Try to make joins as less expensive as possible.
- Data Cleaning, Validation, Enrichment, Analysis and Post Analysis have to be automated. Try using schedulers.
-  Appropriate logs have to maintained to track the behaviour and overcome failures in the pipeline.
    

##	Design

 To solve the Music Data Analysis Requirement the following modules are used

i.	LoadHBaseTables – This is used for loading the lookup data from csv files to the relevant tables in Hbase. Hbase tables used are as below:
-	StationIdGeoCd –  This HBase table is used for storing mapping between stationId and GeoCd
-	SongArtist - This HBase table is used for storing mapping beween Song and Artist
-	UserArtist – This HBase table is used for storing mapping beween userId and List of artists he follows
-	UserSubscription- This HBase table is used for storing mapping beween userId and subscription start timestamp and end timestamp
ii.	MusicDataProcessorApp - This is the main application for processing music data. It in turn calls multiple submodules as below
iii.	WebMusicDataProcessor – This is the class for processing music data stored in /data/web/file-1.xml  folder and use processData method to return dataframes
iv.	MobileMusicDataProcessor - This is the class for processing music data stored in /data/mob/file.txt folder and use processData method to return dataframes
v.	MusicDataEnricher - This is the used for enriching and validating the datasets. New columns are added to dataframe for the processed data. The new columns added to temporary table MusicDataDetailed are as below:
                a.  modified_Geo_cd – This field is used to populate Null/blank values in Geo_cd column. If Geo_cd column is not blank, the value will copied as it is to modified_Geo_cd. If Geo_cd column is  blank then consults the look table StationIdGeoCd based on stationId, If found populate it, else put value Invalid
                b.   modified_Artist_id – This field is used to populate Null/blank values in Artist_id column. If Artist_id column is not blank, the value will copied as it is to modified_Artist_id. If Artist_id column  is  blank then consults the look table SongArtist table based on songId, If found populate it, else put value Invalid
             c. follower – This field is used when the user follows the artist. For each record, for userId consult UserArtist table to find the list of artists user is following. If it is blank, then follower will be 0. If it is not blank and the corresponding artistId for the record is present in artist List from the map, then set follower to 1

    d. subscribed – This filed is used to know if the user was a subscriber when he played the music. This is done by getting the userId for the record and consulting UserSubscription table to get the tuple (start_ts,end_ts). If it does not exist then subscribed field will be 0. If I exists, If the start_ts of record is in between (start_ts,end_ts) of UserSubscritpion then subscribed field will be 1 else it will be 0

e. isValid- This field is used to know if the record is valid or not. Isvalid if valid will have value 1 else 0. The following validations are done:
If User_id is blank then  (0)
If Song_id is blank then 0
If modified_Geo_cd is Invalid then 0
If modified_Arist_id is Invalid then 0
If timestamp is 0 then 0
If Start_ts is 0 then 0
If Start_ts > End_ts then 0 

          
vi.	MusicDataPopulateMapFromLookupTables - This is the used for populating maps from HBase tables used for lookup
vii.	MusicDataAnalyzer - - This is used for analyzing the data in dataframes and store the results in HDFS file:

Top10Stations_<Timestamp> - Top 10 stations where maximum number of songs played which is liked by unique user

MusicDurationByUserType_<Timestmp> - Music Duration by user category of user: Subscribed, Unsubscribed

Top10SongsHavignMaximumRevenue_<Timestamp> - Top 10 songs having maximum revenue

Top10ConnectedArtists_<Timestamp> - Top 10 connected artists

Top10UnsubscribedUsers_<Timestamp> - Top 10 unsubscribed users
    
    
## Implemenation
    
2.	Implementation
MODULE1: LoadHBaseTables:  Loading HBase Tables with Lookup Data

Step1: Start all the services

Start the services using start-all.sh 
Start HBase using:
start-hbase.sh



