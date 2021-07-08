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

i.	LoadHBaseTables– This is used for loading the lookup data from csv files to the relevant tables in Hbase. Hbase tables used are as below:

      a. StationIdGeoCd –  This HBase table is used for storing mapping between stationId and GeoCd	
      b. SongArtist - This HBase table is used for storing mapping beween Song and Artist      
      c. UserArtist – This HBase table is used for storing mapping beween userId and List of artists he follows      
      d. UserSubscription- This HBase table is used for storing mapping beween userId and subscription start timestamp and end timestamp
      
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
   
## Implementation
    
### MODULE1: LoadHBaseTables:  Loading HBase Tables with Lookup Data

#### Step1: Start all the services

Start the services using start-all.sh 
Start HBase using:
start-hbase.sh

#### Step2: Create class for loading Hbase tables
Create a project load-hbase-tables and create a scala class LoadHBaseTables 

Create a project load-hbase-tables and create a scala class LoadHBaseTables which is explained below:

-	Import all the dependent packages and create the package 


#### Step3: Compile the file using eclipse and run the code

-	Define the pom file  
-	Compile using maven install
-	Write a script to load all the tables:

#### Step4: Verify that all the lookup tables are populated correctly

-	Verify all the tables are populated correctly first login to HBase using hbase shell
-	Table StnIdGeoCd is verified using
    scan ‘StnIdGeoCd’
-	Table UserArtist is verified using
    scan ‘UserArtist’
-	Table SongArtist is verified using
scan ‘SongArtist’
-	Table UserSubscription is verified using
scan ‘UserSubscription’

### MODULE2: Handling Web  and Mobile Data

#### Step1: 
       - Write a scala class for handling Web data
       - Write class WebMusicDataProcessor to process web music data stored in /data/web/file-1.xml and store it as dataframe

#### Step2: define class for processing Mobile data

- Define a class MobileMusicDataProcessor for processing music data from /data/mob/file.txt



### MODULE3: Enrich and validate data

- Define a class MusicDataEnricher which does the enrichment and validation of fields in dataframe using UDF



### MODULE4: Analyze the data
- Analyze the data using defining MusicDataAnalyzer class, which will execute sql queries and the result will be stored  as reports in HDFS



### Module5: MusicDataPopulateMapFromLookupTables : Load the maps from HBase tables
To load the HBase table, I used the class MusicDataPopulateMapFromLookupTables

### MODULE6: Main object MusicDataProcessorApp



## OUTPUT:

### All the folders are in HDFS are as below:

![All folders](/pict1.png)

### All Records – This will contain all the records in MusicDataDetailed
![All folders](/pict2.png)

### Top10Stations – This will top 10 stations having maximum music played which are liked by unique users

![Top10UnsubscribedUsers](/pict3.png)

### Top10UnsubscribedUsers – Top 10 unsubscribed users who listened to the music for maximum duration

![Top10UnsubscribedUsers](/pict4.png)


### TotalDurationByCategory – This will report total duration of music played by each time of user (unsubscribed/unsubscribed)

![TotalDurationByCategory](/pict5.png)

### DurationByCategory – This will report total duration of music played by each time of user (unsubscribed/unsubscribed)
![TotalDurationByCategory](/pict6.png)

### Top10ConnectedArtists : Top 10 Connected Artists
![Top10ConnectedArtists](/pict7.png)

### Top10SongsHavingMaximumRevenue: Top 10 songs have maximum revenue

![Top10SongsHavingMaximumRevenue](/pict8.png)


### Top10UnsubscribedUsers – Top 10 unsubscribed users who listened to the music for maximum duration

![Top10UnsubscribedUsers](/pict8.png)



## Module7: Script for running the Music Data App

A bash script music_data_spark_app.sh is created and it content is as below:


#!/bin/bash

export HBASE_PATH=`/usr/local/hbase/bin/hbase classpath`
cd $SPARK_HOME

echo "Starting the Music Data Application"
bin/spark-submit --class final_project.MusicDataProcessorApp --driver-class-path $HBASE_PATH --master local[2] --packages com.databricks:spark-csv_2.10:1.4.0  ~/scala_eclipse/workspace/process-music-data/target/process_music_data-0.0.1-SNAPSHOT.jar > output.log 2>&1

echo "Completed the Music Data Application"


Next, a cron job is created using “crontab –e” so that the script is run every 3 hours
Content is as below
0 */4 * * * /home/acadgild/project_music_data_analysis/music_data_spark_app.sh


## Highlights of the project

i.	No join of query is used while analysiss. Data is already enriched with new fields and using broadcast maps on Lookup tables so as to avoid any join
ii.	Logger has been used in all over the code to allow


## Issues Faced and how to resolve it

While doing project, I face following issues and how I resolved it

i.	XML processing – I found the databricks provides he XML processing APIs in spark. But it was not working. The main issue I face 
 I was trying the process the xml dataset (file-1.xml) to covert to spark dataframe as given for the final project. I have created a scala Object ReadXMLMusicData.scala based on the steps given in

https://github.com/databricks/spark-xml

Created maven project with pom.xml attached. Using mvn install, I am able to generate jar file read_xml_music_data-0.0.1-SNAPSHOT.jar . However when I try to deploy to spark using spark-submit on Acadgild VM

bin/spark-submit --class ReadXMLMusicData --master local[2] --driver-class-path /home/acadgild/.m2/repository/com/databricks/spark-xml_2.10/0.4.1/spark-xml_2.11-0.4.1.jar   ~/scala_eclipse/workspace/read_xml_music_data/target/read_xml_music_data-0.0.1-SNAPSHOT.jar 

Exception in thread "main" java.lang.NoSuchMethodError: org.apache.spark.sql.DataFrameReader.load(Ljava/lang/String;)Lorg/apache/spark/sql/Dataset;
    at ReadXMLMusicData$.main(ReadXMLMusicData.scala:12)
    at ReadXMLMusicData.main(ReadXMLMusicData.scala)
    at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
    at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
    at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
    at java.lang.reflect.Method.invoke(Method.java:497)
    at org.apache.spark.deploy.SparkSubmit$.org$apache$spark$deploy$SparkSubmit$$runMain(SparkSubmit.scala:731)
    at org.apache.spark.deploy.SparkSubmit$.doRunMain$1(SparkSubmit.scala:181)
    at org.apache.spark.deploy.SparkSubmit$.submit(SparkSubmit.scala:206)
    at org.apache.spark.deploy.SparkSubmit$.main(SparkSubmit.scala:121)
    at org.apache.spark.deploy.SparkSubmit.main(SparkSubmit.scala)

I tried  all the options
1. --packages com.databricks:spark-xml_2.10:0.4.1
2. --packages com.databricks:spark-xml_2.11:0.4.1 
3. --driver-class-path /home/acadgild/.m2/repository/com/databricks/spark-xml_2.10/0.4.1/spark-xml_2.10-0.4.1.jar
4. --driver-class-path /home/acadgild/.m2/repository/com/databricks/spark-xml_2.10/0.4.1/spark-xml_2.11-0.4.1.jar




Acadgild gave the following solution:
DataFrame() was supplanted in Spark 2 by Dataset(). You'll need to import org.apache.spark.sql.Dataset and use that if you're running a Spark 1.6 client with a Spark 2.1 server-side. it would be a lot better off using at least Spark 2.1.+ dependencies.

Follow the below link to download newer version of spark.

Spark_Download


But I solved this I used a xml APIs from scala.xml.XML. Relevant links are as below:
https://alvinalexander.com/scala/how-to-extract-data-from-xml-nodes-in-scala 
 https://hadoopist.wordpress.com/2016/01/08/parsing-a-basic-xml-using-hadoop-and-spark-core-apis/ 
 http://www.scala-lang.org/api/2.11.1/scala-xml


ii.	Task Not Serializable Exception:
I am encoutering "org.apache.spark.SparkException: Task Not Serializable"  In the code below when Artist_id is blank I try to get it using Song_Id from broadcastSongArtistMap.  The dataset used is the mobile dataset. Basically exception is thrown in the line below:

if (x(2).equals("")) broadcastSongArtistMap.value.get(x(1)).getOrElse("Invalid") else x(2),

The job is submitted using spark-submit. I would very much appreciate, if you can give me a solution on this ( I tied lot of things, but could not make it work)

Code snippet is as below:

val songArtistMap = Map("S200" -> "A300",
                         "S201" -> "A301",
                         "S202" -> "A302",
                         "S203" -> "A303",
                         "S204" -> "A304",
                         "S205" -> "A301",
                         "S206" -> "A302",
                         "S207" -> "A303",
                         "S208" -> "A304",
                         "S209" -> "A305"
                      )
val broadcastSongArtistMap = sc.broadcast(songArtistMap) 

case class MusicData(User_id:String, Song_id:String, Artist_id:String, Timestamp:Long,
      Start_ts:Long, End_ts:Long, Geo_cd:String, Station_id:String, Song_end_type:String,
      Like:String, Dislike:String)

val recordRDD = sc.textFile("/home/acadgild/final_project/file.txt")
val recordFieldsRDD = recordRDD.map(x => x.split(",")).filter(x=> x.length ==11)
   
val recordDF = recordFieldsRDD.map(x => MusicData(x(0),
        x(1), 
        if (x(2).equals("")) broadcastSongArtistMap.value.get(x(1)).getOrElse("Invalid") else x(2),
        x(3).toLong, 
        x(4).toLong,
        x(5).toLong,
        x(6),
        x(7),
        x(8),
        if (x(9).equals("")) "0" else x(9),
        if (x(10).equals("")) "0" else x(10))).toDF

recordDF.show


Solution: The problem was that SparkContext as parameter to class MobileMusicDataProcessor was not Serializable. When I removed the SparkContext, it worked


iii.	Continue does not work for scala:

There is no support for continue in scala. To use it I created a case class 
CustomException(message:String) extends Exception(message)

Then used 


iv.	HBase support with Spark

I used the following link:

https://acadgild.com/blog/spark-on-hbase/
 
 https://acadgild.com/blog/apache-hbase-beginners-guide/

Even using that the multiple fields were not working. To solve  this, I used the following code:


val hbaseRDD = sc.newAPIHadoopRDD(hconf, classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[org.apache.hadoop.hbase.client.Result])

    val resultRDD = hbaseRDD.map(tuple=>tuple._2)
   
    
    val keyValueRDD = resultRDD.map(result => (Bytes.toString(result.getRow()).split(" ")(0), (
        if (Bytes.toString(result.getValue(new String("Subscription").getBytes(), new String("Start_ts").getBytes)).equals("")) 0 else Bytes.toString(result.getValue(new String("Subscription").getBytes(), new String("Start_ts").getBytes)).toLong,
        if (Bytes.toString(result.getValue(new String("Subscription").getBytes(), new String("End_ts").getBytes)).equals("")) 0 else Bytes.toString(result.getValue(new String("Subscription").getBytes(), new String("End_ts").getBytes)).toLong
        )))

v.	Map was giving compilation error

There are two types of Map Mutable and Immutable

I needed to convert to Immutable map to make the compilation work using the following:

  val map = keyValueRDD.collectAsMap
   return scala.collection.immutable.Map(map.toSeq:_*)


vi.	toDF does not work and giving compilation error
To solve this I needed o add:

import sqlContext.implicits._

To every class

vii.	The following command was giving exception:

sqlContext.resisterTempTable(“MusicDataDetailed”)

The problem was not I was creating sqlContext in every class and dataframe was created tow different classes. To solve this I had to use one sqlContext created at MusicDataProcessorApp

  try {
        if (userId.equals("") && songId.equals("") && artistId.equals("")) {
          throw CustomException("Record is blank")
        } else {
          recordListBuffer += ((userId, songId, artistId, timestampLong, startTsLong, endTsLong, geoCd, stationId, songEndType, like, dislike))
        }
      } catch {
        case CustomException(msg) => msg
      }
    }



viii.	Not able to store query result to file..


To use this I used databricks csv API:

val df = sqlContext.sql("SELECT Songs_id, SUM(duration) AS total_duration_milliseconds FROM SongDuration "
      + " GROUP BY Songs_id ORDER BY total_duration_milliseconds DESC LIMIT 10")
    df.show()
    val df1 = df.repartition(1)
    df1.write.format("com.databricks.spark.csv").option("header", "true").save(reportBasePath + "/Top10SongsHavignMaximumRevenue_" + currentTimestamp)


bin/spark-submit --class final_project.MusicDataProcessorApp --master local[2] --packages com.databricks:spark-csv_2.10:1.4.0 --driver-java-options "-Dlog4j.configuration=file:/usr/local/spark/spark-1.6.0-bin-hadoop2.6/conf/log4j.properties" ~/scala_eclipse/workspace/process-music-data/target/process_music_data-0.0.1-SNAPSHOT.jar > output.log 2>&1


ix.	AnalysisException whenever I used a field which does not exist because of case. For example Song_Id, but the schma it is Song_id. Even if I restart or whatever I does not work. I needed to change the schema to Songs_id to solve this













 
   
 






