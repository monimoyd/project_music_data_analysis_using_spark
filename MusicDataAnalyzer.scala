package final_project
import org.apache.log4j.{ Level, LogManager, PropertyConfigurator }
import org.apache.spark.sql.AnalysisException
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.HashMap

class MusicDataAnalyzer(context: SparkContext, sqc: SQLContext, musicDataDFParam: DataFrame) extends Serializable {

  val musicDataDF: DataFrame = musicDataDFParam
  val sc = context
  val sqlContext = sqc

  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)
  val reportBasePath = "/user/acadgild/project_music_data_analysis/reports/"
  val currentTimestamp = System.currentTimeMillis().toString

  def analyze() = {
    import sqlContext.implicits._
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
  
    def getAllRecords() {
     log.info(" Get All records")
     val df = sqlContext.sql("SELECT * FROM MusicDataDetailed ")
     
    df.show()
    val df1 = df.repartition(1)
    df1.write.format("com.databricks.spark.csv").option("header", "true").save(reportBasePath + "/MusicDataAllRecords_" + currentTimestamp)

  

  }
  

  def getTop10Stations() {
    log.info(" Top 10 Music Stations where maximum numbers of songs played which are liked by unique users")
    sqlContext.sql("SELECT Station_id, User_id, count(*) AS music_count FROM MusicDataDetailed "
      + " WHERE Likes='1' AND isValid='1' GROUP BY Station_id, User_id")
      .registerTempTable("MusicCountByStation")
    sqlContext.sql("SELECT Station_id, User_id, CASE WHEN music_count> 1 THEN 1 ELSE music_count END "
      + " AS unique_music_count FROM MusicCountByStation")
      .registerTempTable("UniqueMusicCountByStation")
    val df = sqlContext.sql("SELECT Station_id, sum(unique_music_count) AS total_music_count "
      + " FROM UniqueMusicCountByStation GROUP BY Station_id ORDER BY total_music_count "
      + " DESC LIMIT 10 ")
    df.show()
    val df1 = df.repartition(1)
    df1.write.format("com.databricks.spark.csv").option("header", "true").save(reportBasePath + "/Top10Stations_" + currentTimestamp)

  }

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