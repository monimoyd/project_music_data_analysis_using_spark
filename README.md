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
      
      c.	UserArtist – This HBase table is used for storing mapping beween userId and List of artists he follows
      
      d. 	UserSubscription- This HBase table is used for storing mapping beween userId and subscription start timestamp and end timestamp
      
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
    
MODULE1: LoadHBaseTables:  Loading HBase Tables with Lookup Data

Step1: Start all the services

Start the services using start-all.sh 
Start HBase using:
start-hbase.sh

Step2: Create class for loading Hbase tables
Create a project load-hbase-tables and create a scala class LoadHBaseTables 

Create a project load-hbase-tables and create a scala class LoadHBaseTables which is explained below:

-	Import all the dependent packages and create the package 

package final_project

import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.{ Put, HTable }
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.log4j.{Level, LogManager, PropertyConfigurator}

-	Create object LoadHBaseTables and define main method and logs
-	

object LoadHBaseTables {
  def main(args: Array[String]) {
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
   
-	Get all the arguments, argument 1 is for filePath of lookup file, argument 2 is table name, argument 3 is composite  fields of separated fields of Column Family and Column, which can be multiple

    val filePath = args(0)
    val tablename = args(1)
    val columnFamilyField = args(2)
    val columnFamilyFieldList = columnFamilyField.split(",")
    val columnFamilyFieldListLength = columnFamilyFieldList.length
-	Create configuration, Spark Context, SQL Context
    val conf = new SparkConf().setAppName("LoadHbaseTable").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)

-	Create HBase configuration
    val hconf = HBaseConfiguration.create
    hconf.set(TableInputFormat.INPUT_TABLE, tablename)
    val admin = new HBaseAdmin(hconf)
-	Check if tables is already created or not . If not created, create table and its column family
    if (!admin.isTableAvailable(tablename)) {

      log.info("Creating table " + tablename)
      val tableDescription = new HTableDescriptor(tablename)
      tableDescription.addFamily(new HColumnDescriptor(columnFamilyFieldList(0).getBytes()))
      admin.createTable(tableDescription)
      if (admin.isTableAvailable(tablename)) {
         log.info("Table " + tablename + " is created successfully")
      }
    } else {
      log.warn("Table " + tablename + " already exists")
    }
    
-	Get the table and  
   
    val table = new HTable(hconf, tablename)

-	Process the csv file which has lookup data and create tuples using map  with key and value. If there are multiple fields given, value is taken as a tuple
    
    val file = sc.textFile("file://" + filePath)
    
    var records1:Array[(String,String)] = null
    var records2:Array[(String,String,String)] = null
    if (columnFamilyFieldListLength == 2) {
        records1 = file.map(_.split(",")).map(x => (x(0), x(1))).collect
    } else if (columnFamilyFieldListLength == 4) {
        records2 = file.map(_.split(",")).map(x => (x(0), x(1), x(2))).collect
    }
-	Save the columns    
    if (columnFamilyFieldListLength == 2) {
      for(record <- records1) {
         var p = new Put(new String(record._1).getBytes())
          p.add(columnFamilyFieldList(0).getBytes(), columnFamilyFieldList(1).getBytes(), new String(record._2).getBytes())
          table.put(p)
      }
    } else if (columnFamilyFieldListLength == 4) {
        for(record <- records2) {
           var p = new Put(new String(record._1).getBytes())
           p.add(columnFamilyFieldList(0).getBytes(), columnFamilyFieldList(1).getBytes(), new String(record._2).getBytes())
           table.put(p)
           var q = new Put(new String(record._1).getBytes())
           q.add(columnFamilyFieldList(2).getBytes(), columnFamilyFieldList(3).getBytes(), new String(record._3).getBytes())
           table.put(q)
        }
      
    }
  
    table.flushCommits()

   
  }

}


## Step3: Compile the file using eclipse and run the code

-	Define the pom file  
-	Compile using maven install
-	Write a script to load all the tables:

Step4: Verify that all the lookup tables are populated correctly

-	Verify all the tables are populated correctly first login to HBase using hbase shell
-	Table StnIdGeoCd is verified using
    scan ‘StnIdGeoCd’
-	Table UserArtist is verified using
    scan ‘UserArtist’
-	Table SongArtist is verified using
scan ‘SongArtist’
-	Table UserSubscription is verified using
scan ‘UserSubscription’

## MODULE2: Handling Web  and Mobile Data

Step1: Write a scala class for handling Web data
-	Write class WebMusicDataProcessor to process web music data stored in /data/web/file-1.xml and store it as dataframe

-Import dependent packages

package final_project
import org.apache.spark._
import scala.xml.XML
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.log4j.{ Level, LogManager, PropertyConfigurator }
import org.apache.spark.broadcast.Broadcast
import java.text.SimpleDateFormat
import scala.collection.mutable.HashMap

// Define CustomException this is to solve the continue 

case class CustomException(message:String) extends Exception(message)

// Define the class with WebMusicDataProcessor with parametes
class WebMusicDataProcessor(param: String, context: SparkContext, sqc: SQLContext) extends Serializable {
  val filePath: String = param
  val sc: SparkContext = context
  val sqlContext:SQLContext = sqc


// Define the method which does the processing of data  and return as dataframe

  
  def processData(): DataFrame = {
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
  //  val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
// Define dateFormat as   yyyy-MM-dd HH:mm:ss and  recordListBuffer which will act as buffer for storing data
 val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val recordListBuffer = new ListBuffer[(String, String, String, Long, Long, Long, String, String, String, String, String)]()

// Load data from XML path
        
    val xml = XML.loadFile(filePath)

-	Process all the fields user_id, song_id, artist_id, timestamp, start_ts,end_ts. Handle all null conditions
    for (tag <- xml.child) {
      val userId = (tag \ "user_id").text
      val songId = (tag \ "song_id").text
      var artistId = (tag \ "artist_id").text
          
      val timestamp = (tag \ "timestamp").text
      var timestampLong:Long = 0
      if (!timestamp.equals("")) {
        timestampLong = dateFormat.parse(timestamp).getTime()
      }
      val startTs = (tag \ "start_ts").text
      var startTsLong:Long = 0
      if (!startTs.equals("")) {
          startTsLong = dateFormat.parse(startTs).getTime()
      }
      val endTs = (tag \ "end_ts").text
      var endTsLong:Long  = 0
      if (!endTs.equals("")) {
          endTsLong = dateFormat.parse(endTs).getTime()
      }
      var geoCd = (tag \ "geo_cd").text
     
      val stationId = (tag \ "station_id").text
      
          
      val songEndType = (tag \ "song_end_type").text
      var like = (tag \ "like").text
      if (like.equals("")) like = "0"
      var dislike = (tag \ "dislike").text
      if (dislike.equals("")) dislike = "0"
      try {

// Continue with the record in case fileds are blank
        if (userId.equals("") && songId.equals("") && artistId.equals("")) {
          throw CustomException("Record is blank")
        } else {
          recordListBuffer += ((userId, songId, artistId, timestampLong, startTsLong, endTsLong, geoCd, stationId, songEndType, like, dislike))
        }
      } catch {
        case CustomException(msg) => msg
      }
    }

// From the buffer convert to dataFrame recordDF having fields User_id, Songs_id, Artist_id, Timestamp,      // Start_ts, End_ts, Geo_cd, Station_id, Song_end_type,       Likes, Dislikes
    val recordList = recordListBuffer.toList
         
    val recordRDD = sc.parallelize(recordList)
    val recordDF = recordRDD.toDF("User_id", "Songs_id", "Artist_id", "Timestamp",
      "Start_ts", "End_ts", "Geo_cd", "Station_id", "Song_end_type",
       "Likes", "Dislikes")
       
    log.info("Number of records =" + recordDF.count)
    log.info("Showing records for Web Music Data ")
    recordDF.show

    return recordDF
    
  }
  
  Step2: define class for processing Mobile data

-	Define a class MobileMusicDataProcessor for processing music data from /data/mob/file.txt

// define package as final_project and Import  al the dependent packages
package final_project

import org.apache.spark._
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.log4j.{ Level, LogManager, PropertyConfigurator }
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable.HashMap

// Create a case class MusicData with all the fields

case class MusicData(User_id:String, Songs_id:String, Artist_id:String, Timestamp:Long,
      Start_ts:Long, End_ts:Long, Geo_cd:String, Station_id:String, Song_end_type:String,
      Likes:String, Dislikes:String)

// Define class MobileMusicDataProcessor with all the parameters

class MobileMusicDataProcessor(param: String, context: SparkContext, sqc:SQLContext) extends Serializable {
  val filePath: String = param
  val sc: SparkContext = context
  val sqlContext:SQLContext = sqc
  
  // Define method processData 
  def processData(): DataFrame = {
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    
    import sqlContext.implicits._
// Load dataset from mobeile file path /data/mob/file.txt into RDD and slipt the fields
    val recordRDD = sc.textFile(filePath)
    val recordFieldsRDD = recordRDD.map(x => x.split(",")).filter(x=> x.length ==11)
   
    // Convert the RDD to dataframe with case class MusicData
    val recordDF = recordFieldsRDD.map(x => MusicData(x(0),
        x(1), 
        x(2),
        if (x(3).equals("")) 0 else x(3).toLong,
        if (x(4).equals("")) 0 else x(4).toLong,
        if (x(5).equals("")) 0 else x(5).toLong,
        x(6),
        x(7),
        x(8),
        if (x(9).equals("")) "0" else x(9),
        if (x(10).equals("")) "0" else x(10))).toDF
       
        

    log.info("Number of records for Mobile Music Data =" + recordDF.count)
    log.info("Showing records for Mobile Music Data ")
    recordDF.show

    return recordDF

  }

}


MODULE3: Enrich and validate data

Step1: Define a class MusicDataEnricher which does the enrichment and validation of fields in dataframe using UDF

// Define package final_project and import all the dependent packages
package final_project

import org.apache.spark._
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.log4j.{ Level, LogManager, PropertyConfigurator }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.udf
import scala.collection.mutable.HashMap

// Define class MusicDataEnricher with the parmeters of all SparkContext, DataFrame and all the
 // broadcast maps which has lookup data
class MusicDataEnricher( allDataFrameParam: DataFrame, broadcastStndIdGeoCdMapParam:Broadcast[Map[String, String]], broadcastSongArtistMapParam:Broadcast[Map[String, String]], broadcastUserArtistMapParam: Broadcast[Map[String, String]], broadcastUserSubscriptionParam: Broadcast[Map[String, (Long, Long)]]) extends Serializable {
	val allDataFrame: DataFrame =allDataFrameParam
 // val stndIdGeoCdMap:HashMap[String, String] = stndIdGeoCdMapParam
	val broadcastStndIdGeoCdMap:Broadcast[Map[String, String]] = broadcastStndIdGeoCdMapParam
	val broadcastSongArtistMap:Broadcast[Map[String, String]] = broadcastSongArtistMapParam
	val broadcastUserArtistMap:Broadcast[Map[String, String]] = broadcastUserArtistMapParam
// The UDF method  fillNullValueGeoCd wlll take stationId and geoCd as parameter and if geoCd is not
 // blank then it will take as it is. If geoCd is blank it will use the lookup map  broadcastStndIdGeoCdMap 
// using stationId, get the geoCd. If it is not there record will be marked as Invalid
// A new field modified_Geo_cd will be added to the dataframe
	def fillNullValueGeoCd = udf((stationId: String, geoCd: String) => {
	                       
		if (!geoCd.equals("")) geoCd
		else {
		  //  val geoCdVal = stndIdGeoCdMap.get(stationId).getOrElse("Invalid")
		  val geoCdVal = broadcastStndIdGeoCdMap.value.get(stationId).getOrElse("Invalid")
			 geoCdVal
		}
	})

// The UDF method  fillNullValueArtistId wlll take songId and artistId as parameter and if artistId is not
 // blank then it will take as it is. If artistId is blank it will use the lookup map  broadcastSongArtistMap 
// using songId, get the artistId. If it is not there record will be marked as Invalid
// A new field modified_Artist_id will be added to the dataframe

def fillNullValueArtistId = udf((songId: String, artistId: String) => {
	if (!artistId.equals("")) artistId
	else {
		val artistIdVal = broadcastSongArtistMap.value.get(songId).getOrElse("Invalid")
    artistIdVal
	}
})

// The UDF method  findFollowers wlll take userId and artistId as parameter. 
// Based on usedId from the map broadcastUserArtistMap, artistList is retrieved. If aritstId list is blank 
// then 0 will be retuned. If artistList is not  blank then it will be split based on & and create a array
 // artistArray. If it contains  using artistId, 1 will be returned, else 0 will be returned
// A new field follower will be added to the dataframe


def findFollowers = udf((userId: String, artistId: String) => {
	val artistList = broadcastUserArtistMap.value.get(userId).getOrElse("")
if (artistList.equals("")) "0"
	else {
		var artistArray = artistList.split("&")
				if (artistArray contains artistId) "1"
					else "0"

	}

})
// The UDF method  findSubscribers wlll take userId and starts as parameter. Basedon userId
// lokkup is done on broadcastUserSubscription.value.get(userId). If subscription does not exits, it 
// returns 0. If it exists and starts is in between subscription start time and end time return 1 else 
/ /retun 0. A new field subscribed is added to the dataframe

def findSubscribers = udf((userId: String, startTs: Long) => {

	val subscriptionTuple= broadcastUserSubscription.value.get(userId).getOrElse((0L, 0L))
  if (subscriptionTuple._1 ==0 && subscriptionTuple._2 == 0 ) "0"
	else if (startTs >= subscriptionTuple._1  && startTs <=  subscriptionTuple._2) "1"
	else "0"

})

// Using UDF validate records validation is done. If userId, songId are blank return 0. If modifiedArtistId // or modifiedGeoCdi si Invalid return 0,. If timestamp or start_ts is 0 then 0. If end_ts is less than // start_ts return 0. Else return 1. Add a new field isValid to the dataframe
def validateRecords = udf((userId: String, songId: String, modifiedArtistId: String, modifiedGeoCd: String, timestamp:Long, start_ts:Long, end_ts:Long) => {
	if (userId.equals("")) "0"
		else if (songId.equals("")) "0"
			else if (modifiedArtistId.equals("Invalid")) "0"
			  else if (modifiedGeoCd.equals("Invalid")) "0"
				else if (timestamp == 0) "0"
					else if(start_ts == 0) "0"
						else if (end_ts < start_ts) "0"
							else "1"
})




// The method enrichData will execute all the UDFs defined above and enrich dataframe with new fields
def enrichData():DataFrame = {
	 
 var newDataFrame = allDataFrame.withColumn("modified_Geo_cd",  fillNullValueGeoCd(allDataFrame("Station_id"), allDataFrame("Geo_cd")))
 newDataFrame = newDataFrame.withColumn("modified_Artist_id",  fillNullValueArtistId(newDataFrame("Songs_id"), newDataFrame("Artist_id")))
 newDataFrame = newDataFrame.withColumn("follower",  findFollowers(newDataFrame("User_id"), newDataFrame("modified_Artist_id")))
 newDataFrame = newDataFrame.withColumn("subscribed", findSubscribers(newDataFrame("User_id"), newDataFrame("Start_ts")))
 newDataFrame = newDataFrame.withColumn("isValid", validateRecords (newDataFrame("User_id"), newDataFrame("Songs_id"), newDataFrame("modified_Artist_id"),  newDataFrame("modified_Geo_cd"), newDataFrame("Timestamp"), newDataFrame("Start_ts"), newDataFrame("End_ts")))

 return newDataFrame
}

}




## MODULE4: Analyze the data
-	Analyze the data using defining MusicDataAnalyzer class, which will execute sql queries and the result will be stored  as reports in HDFS

// Define the package final_project and import all the dependent packages

package final_project
import org.apache.log4j.{ Level, LogManager, PropertyConfigurator }
import org.apache.spark.sql.AnalysisException
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.HashMap

// Define the class MusicDataAnalyzer with parameters SparkContext, SQLContext and DataFrame

class MusicDataAnalyzer(context: SparkContext, sqc: SQLContext, musicDataDFParam: DataFrame) extends Serializable {

  val musicDataDF: DataFrame = musicDataDFParam
  val sc = context
  val sqlContext = sqc

// Define logger and reportBasePath and currentTimestamp
  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)
  val reportBasePath = "/user/acadgild/project_music_data_analysis/reports/"
  val currentTimestamp = System.currentTimeMillis().toString

// Define method analyze which will in run call all the methods for analysis
// While calling method, each one is put in try catch block so that failing one does not impact others

  def analyze() = {
    import sqlContext.implicits._

// Define temporary table MusicDataDetailed on the dataframe
    musicDataDF.registerTempTable("MusicDataDetailed")
    log.info("Before calling getAllRecords()")
    
    try {
      getAllRecords()
    } catch {
      case e: Exception => log.error("Exception got while calling getAllRecords: " + e)
    }    
    log.info("Before calling getTop10Stations()")
    
    try {
      getTop10Stations()
    } catch {
      case e: Exception => log.error("Exception got while calling getTop10Stations: " + e)
    }
    log.info("Before calling getMusicDurtionByUser()")
    
    try {
      getMusicDurtionByUserType()
    } catch {
      case e: Exception => log.error("Exception got while calling getTop10Stations: " + e)
    }
    log.info("Before calling getTo10ConnectedArtists()")
    
    try {
      getTop10ConnectedArtists()
    } catch {
      case e: Exception => log.error("Exception got while calling getTop10ConnectedArtists: " + e)
    }
    
    log.info("Before calling  getTop10UnsubscribedUsers()")
    
    
    try {
      getTop10UnsubscribedUsers()
    } catch {
      case e: Exception => log.error("Exception got while calling getTop10UnsubscribedUsers(): " + e)
    }
    
    log.info("Before calling  getTo10SongsHavignMaximumRevenue()")
    try {
      getTop10SongsHavingMaximumRevenue()
    } catch {
      case e: Exception => log.error("Exception got while calling getTop10SongsHavingMaximumRevenue(): " + e)
    }

  }

// Method getAllRecords will select all the records from MusicDataDetailed  

    def getAllRecords() {
     log.info(" Get All records")
// Run the query to get the result
     val df = sqlContext.sql("SELECT * FROM MusicDataDetailed ")
     
    df.show()

// Store the result as a single file in HDFS with reportPath and report name MusicDataAllRecords concatenated with current timestamp
    val df1 = df.repartition(1)
    df1.write.format("com.databricks.spark.csv").option("header", "true").save(reportBasePath + "/MusicDataAllRecords_" + currentTimestamp)

  

  }
  
// The method getTop10Stations will return top 10 stations were maximum songs are played which are 
// liked by unique user
  def getTop10Stations() {
    log.info(" Top 10 Music Stations where maximum numbers of songs played which are liked by unique users")

// Execute query to get Station_id User_id and count of music played Group by Station_id, User_id and // Likes is 1 and isValid is 1
    sqlContext.sql("SELECT Station_id, User_id, count(*) AS music_count FROM MusicDataDetailed "
      + " WHERE Likes='1' AND isValid='1' GROUP BY Station_id, User_id")
      .registerTempTable("MusicCountByStation")

// Execute query to get unique unique count for user liked, so music_count is greater than 1, it is 
// considered 1

    sqlContext.sql("SELECT Station_id, User_id, CASE WHEN music_count> 1 THEN 1 ELSE music_count END "
      + " AS unique_music_count FROM MusicCountByStation")
      .registerTempTable("UniqueMusicCountByStation")
// Using sum method of SQL, aggregate the total music count group by Station_id and order 
// total_music_count desceding. Take first 10 records

    val df = sqlContext.sql("SELECT Station_id, sum(unique_music_count) AS total_music_count "
      + " FROM UniqueMusicCountByStation GROUP BY Station_id ORDER BY total_music_count "
      + " DESC LIMIT 10 ")
    df.show()

// Store the query output to HDFS as a csv report Top10Stations concatenated with timestamp in
 // reportBasePath 
    val df1 = df.repartition(1)
    df1.write.format("com.databricks.spark.csv").option("header", "true").save(reportBasePath + "/Top10Stations_" + currentTimestamp)

  }

// The method getMusicDurtionByUserType returns total lke music by each category of user Subscribed 
// or Unsubscribed
  def getMusicDurtionByUserType() {
    log.info(" Total duration of Songs played by Subscibed and Unsubsribed Users")
  
    sqlContext.sql("SELECT CASE WHEN subscribed='1' THEN 'Subscribed' ELSE 'Unsubscribed' END AS User_type, (End_ts -Start_ts) AS duration  "
      + " FROM MusicDataDetailed WHERE isValid='1'")
      .registerTempTable("UserTypeDuration")
    val df = sqlContext.sql("SELECT User_type, SUM(duration) AS total_duration_milliseconds FROM UserTypeDuration "
      + " GROUP BY User_type ORDER BY total_duration_milliseconds DESC")
    df.show()
    val df1 = df.repartition(1)

    df1.write.format("com.databricks.spark.csv").option("header", "true").save(reportBasePath + "/MusicDurationByUserType_" + currentTimestamp)

  

  }
// The method getTop10ConnectedArtists return top 10 connected artists who are followed by user
  def getTop10ConnectedArtists() {
    log.info(" Top 10 Connected Artists")
    sqlContext.sql("SELECT Artist_id, User_id, count(*) AS music_count FROM MusicDataDetailed "
      + " WHERE follower='1' AND isValid='1' GROUP BY Artist_id, User_id")
      .registerTempTable("MusicCountByArtist")
    sqlContext.sql("SELECT Artist_id, User_id, CASE WHEN music_count> 1 THEN 1 ELSE music_count END "
      + " AS unique_music_count FROM MusicCountByArtist")
      .registerTempTable("UniqueMusicCountByUser")
    val df = sqlContext.sql("SELECT Artist_id, sum(unique_music_count) AS total_music_count "
      + " FROM UniqueMusicCountByUser GROUP BY Artist_id ORDER BY total_music_count "
      + " DESC LIMIT 10 ")
    df.show()
    val df1 = df.repartition(1)
    df1.write.format("com.databricks.spark.csv").option("header", "true").save(reportBasePath + "/Top10ConnectedArtists_" + currentTimestamp)
   

  }
// The method getTop10SongsHavingMaximumRevenue returns top 10 songs having maximum royalty r
//revenue
  def getTop10SongsHavingMaximumRevenue() {
    log.info(" Top 10 Songs Having maximum revenue")
   
    sqlContext.sql("SELECT Songs_id, CASE WHEN End_ts is NOT NULL AND Start_ts is NOT NULL and End_ts > Start_ts THEN End_ts - Start_ts ELSE 0 END AS duration FROM MusicDataDetailed "
      + " WHERE (Likes='1' OR Song_end_type = '0') AND isValid='1' ")
      .registerTempTable("SongDuration")
    val df = sqlContext.sql("SELECT Songs_id, SUM(duration) AS total_duration_milliseconds FROM SongDuration "
      + " GROUP BY Songs_id ORDER BY total_duration_milliseconds DESC LIMIT 10")
    df.show()
    val df1 = df.repartition(1)
    df1.write.format("com.databricks.spark.csv").option("header", "true").save(reportBasePath + "/Top10SongsHavignMaximumRevenue_" + currentTimestamp)
  

  }

// The method getTop10UnsubscribedUsers will retrun top 10 unsubscribed users
  def getTop10UnsubscribedUsers() {
    log.info(" Top 10 Unsubscribed Users")
   
    sqlContext.sql("SELECT User_id,CASE WHEN End_ts is NOT NULL AND Start_ts is NOT NULL and End_ts > Start_ts THEN End_ts - Start_ts ELSE 0 END AS duration FROM MusicDataDetailed "
      + " WHERE subscribed='0' AND isValid='1'")
      .registerTempTable("SongDuration")
    val df = sqlContext.sql("SELECT User_id, SUM(duration) AS total_duration_milliseconds FROM SongDuration "
      + " GROUP BY User_id ORDER BY total_duration_milliseconds DESC LIMIT 10")

    df.show()
    val df1 = df.repartition(1)
    df1.write.format("com.databricks.spark.csv").option("header", "true").save(reportBasePath + "/Top10UnsubscribedUsers_" + currentTimestamp)
   

  }

}


## Module5: MusicDataPopulateMapFromLookupTables : Load the maps from HBase tables
To load the HBase table, I used the class MusicDataPopulateMapFromLookupTables

// Import the dependent packages
package final_project
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.{Put,HTable}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.log4j.{ Level, LogManager, PropertyConfigurator }

// Create class MusicDataPopulateMapFromLookupTables with parameters
class MusicDataPopulateMapFromLookupTables(context: SparkContext) {
  val sc:SparkContext = context  
  val hconf = HBaseConfiguration.create
  val admin = new HBaseAdmin(hconf)
  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)
    // Create map from HBase table with key and value as column value
  def getCommonLookupMap(tablename:String) = {
      if (!admin.isTableAvailable(tablename)) {
        log.warn("HBase Table " + tablename + "  does not exists")
    } else {
        log.info("Table " + tablename + " exists")
    }
    
    val hbaseRDD = sc.newAPIHadoopRDD(hconf, classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[org.apache.hadoop.hbase.client.Result])

    val resultRDD = hbaseRDD.map(tuple=>tuple._2)
    val keyValueRDD = resultRDD.map(result => (Bytes.toString(result.getRow()).split(" ")(0), Bytes.toString(result.value)))
 
    val map = keyValueRDD.collectAsMap
    scala.collection.immutable.Map(map.toSeq:_*)

  }
  // Populate Map with key StationId and GeoCd from table StndIdGeoCd
  def getStationIdGeoCdMap() = {
    val tablename = "StndIdGeoCd"
    hconf.set(TableInputFormat.INPUT_TABLE, tablename)
    getCommonLookupMap(tablename)
    
    
  }
  
// Populate Map with key songId and Artist from table SongArtist
  def getSongArtistMap() = {
    val tablename = "SongArtist"
    hconf.set(TableInputFormat.INPUT_TABLE, tablename)
    getCommonLookupMap(tablename)    
    
  }
  
// Populate Map with key userId and Artist List from table UserArtist
  
  def getUserArtistMap() = {
    val tablename = "UserArtist"
    hconf.set(TableInputFormat.INPUT_TABLE, tablename)
    getCommonLookupMap(tablename)   
    
  }
  // Populate map with Key as UserId and Value as tuple (start_ts, end_ts)
  def getUserSubscriptionMap():scala.collection.immutable.Map[String, (Long, Long)] = {
    val tablename = "UserSubscription"
    hconf.set(TableInputFormat.INPUT_TABLE, tablename)
    if (!admin.isTableAvailable(tablename)) {
        log.warn("HBase Table " + tablename + "  does not exists")
    } else {
        log.info("Table " + tablename + " exists")
    }
    
    val hbaseRDD = sc.newAPIHadoopRDD(hconf, classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[org.apache.hadoop.hbase.client.Result])

    val resultRDD = hbaseRDD.map(tuple=>tuple._2)
   
    
    val keyValueRDD = resultRDD.map(result => (Bytes.toString(result.getRow()).split(" ")(0), (
        if (Bytes.toString(result.getValue(new String("Subscription").getBytes(), new String("Start_ts").getBytes)).equals("")) 0 else Bytes.toString(result.getValue(new String("Subscription").getBytes(), new String("Start_ts").getBytes)).toLong,
        if (Bytes.toString(result.getValue(new String("Subscription").getBytes(), new String("End_ts").getBytes)).equals("")) 0 else Bytes.toString(result.getValue(new String("Subscription").getBytes(), new String("End_ts").getBytes)).toLong
        )))
  
    val map = keyValueRDD.collectAsMap
    return scala.collection.immutable.Map(map.toSeq:_*)

    
  }

   
}


## MODULE6: Main object MusicDataProcessorApp

// Import all the dependent packages
package final_project
import org.apache.log4j.{ Level, LogManager, PropertyConfigurator }
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.HashMap

// define object MusicDataProcessorApp
object MusicDataProcessorApp {

// define method main
  def main(args: Array[String]) {
    
  
    val web_file_path = "/data/web/file-1.xml"
    val mobile_file_path = "file:///data/mob/file.txt"
    
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)

// Define all the contexts
    val conf = new SparkConf().setAppName("MusicDataProcessApp").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    sc.setLogLevel("ERROR")
    // Populate the map from StationId GeoCd and create a broadcast object
    val populateMusicDataMap = new MusicDataPopulateMapFromLookupTables(sc)
    val stndIdGeoCdMap:Map[String, String] = populateMusicDataMap.getStationIdGeoCdMap()
    val broadcastStndIdGeoCdMap = sc.broadcast(stndIdGeoCdMap)                     
    
    // Populate the map songArtistMap  from songId Aritist and create a broadcast object
                  
    val songArtistMap =  populateMusicDataMap.getSongArtistMap()
    val broadcastSongArtistMap = sc.broadcast(songArtistMap) 
       // Populate the map userArtistMap  from userId  and Aritist and create a broadcast object

    val userArtistMap = populateMusicDataMap.getUserArtistMap()
    val broadcastUserArtistMap = sc.broadcast(userArtistMap) 
 
       // Populate the map userSubscription from userId  and subscription tuple (start_ts, end_ts) and 
// create a broadcast object
    val userSubscription:Map[String, (Long, Long)] = populateMusicDataMap.getUserSubscriptionMap()
    val broadcastuserSubscription = sc.broadcast(userSubscription)                          
                           
// Call the processData method on WebMusicDataProcessor and get the dataframe webDataFrame
    val webMusicDataProcessor = new WebMusicDataProcessor(web_file_path, sc, sqlContext )
    
    log.info("Before calling webMusicDataProcessor.processData()")
    val webDataFrame:DataFrame = webMusicDataProcessor.processData()
    log.info("After calling webMusicDataProcessor.processData()")
   
// Call the processData method on MobileMusicDataProcessor and get the dataframe mobileDataFrame
     val mobileMusicDataProcessor = new MobileMusicDataProcessor(mobile_file_path, sc, sqlContext)
    
    log.info("Before calling mobileMusicDataProcessor.processData()")
    val mobileDataFrame:DataFrame = mobileMusicDataProcessor.processData()
    log.info("After calling webMusicDataProcessor.processData()")
    
// Combine webDataFrame and mobileDataFrame into one allDataFrame
    val allDataFrame:DataFrame = webDataFrame.unionAll(mobileDataFrame)
    log.info("allDataFrame count =" + allDataFrame.count)
    
// Call the enrichData method on MusicDataEnricher and enhance the data
     val musicDataEnricher = new MusicDataEnricher(allDataFrame, broadcastStndIdGeoCdMap, broadcastSongArtistMap, broadcastUserArtistMap, broadcastuserSubscription) 
    
    val enrichedAllDataFrame:DataFrame = musicDataEnricher.enrichData()
     log.info("Showing dataframe after enrichment")
     enrichedAllDataFrame.show(100)
      enrichedAllDataFrame.registerTempTable("DetailedMusicData")
     // Call the analyze method on musicDataAnalyzer
     val musicDataAnalyzer = new MusicDataAnalyzer(sc, sqlContext, enrichedAllDataFrame)
     musicDataAnalyzer.analyze()
  }
  

}

COMPILATION: pom.xml file is as below:

<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>final_project</groupId>
    <artifactId>process_music_data</artifactId>
    <version>0.0.1-SNAPSHOT</version>
    <properties>
        <maven.compiler.source>1.8</maven.compiler.source>
        <maven.compiler.target>1.8</maven.compiler.target>
        <encoding>UTF-8</encoding>
        <scala.version>2.10.5</scala.version>
    </properties>
    <build>
        <sourceDirectory>src/main/scala</sourceDirectory>
        <resources>
            <resource>
                <directory>src/main/scala</directory>
                <excludes>
                    <exclude>**/*.java</exclude>
                </excludes>
            </resource>
        </resources>
        <plugins>
            <plugin>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.5.1</version>
                <configuration>
                    <source>1.8</source>
                    <target>1.8</target>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.scala-tools</groupId>
                <artifactId>maven-scala-plugin</artifactId>
                <version>2.15.2</version>
                <executions>
                    <execution>
                        <goals>
                            <goal>compile</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
    <dependencies>
        <!-- https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11 -->
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_2.10</artifactId>
            <version>1.6.0</version>
        </dependency>
        <!-- https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.11 -->
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_2.10</artifactId>
            <version>1.6.0</version>
        </dependency>
         <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-hive_2.10</artifactId>
            <version>1.6.0</version>
            <scope>provided</scope>
        </dependency>        
        
        <!-- https://mvnrepository.com/artifact/org.apache.hbase/hbase-client -->
        <dependency>
            <groupId>org.apache.hbase</groupId>
            <artifactId>hbase-client</artifactId>
            <version>0.98.14-hadoop2</version>
        </dependency>
        <!-- https://mvnrepository.com/artifact/org.apache.hbase/hbase-common -->
        <dependency>
            <groupId>org.apache.hbase</groupId>
            <artifactId>hbase-common</artifactId>
            <version>0.98.14-hadoop2</version>
        </dependency>
        <!-- https://mvnrepository.com/artifact/org.apache.hbase/hbase-protocol -->
        <dependency>
            <groupId>org.apache.hbase</groupId>
            <artifactId>hbase-protocol</artifactId>
            <version>0.98.14-hadoop2</version>
        </dependency>
        <!-- https://mvnrepository.com/artifact/org.apache.hbase/hbase-hadoop2-compat -->
        <dependency>
            <groupId>org.apache.hbase</groupId>
            <artifactId>hbase-hadoop2-compat</artifactId>
            <version>0.98.14-hadoop2</version>
        </dependency>
        <!-- https://mvnrepository.com/artifact/org.apache.hbase/hbase-annotations -->
        <dependency>
            <groupId>org.apache.hbase</groupId>
            <artifactId>hbase-annotations</artifactId>
            <version>0.98.14-hadoop2</version>
        </dependency>
        <!-- https://mvnrepository.com/artifact/org.apache.hbase/hbase-server -->
        <dependency>
            <groupId>org.apache.hbase</groupId>
            <artifactId>hbase-server</artifactId>
            <version>0.98.14-hadoop2</version>
        </dependency>
</dependencies>

</project>


OUTPUT:

All the folders are in HDFS are as below:






 
   
 






