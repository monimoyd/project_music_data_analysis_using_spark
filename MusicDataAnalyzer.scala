package final_project
import org.apache.log4j.{ Level, LogManager, PropertyConfigurator }
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.HashMap

class MusicDataAnalyzer(context: SparkContext, sqc: SQLContext, musicDataDFParam:DataFrame)  extends Serializable{
  
  val musicDataDF:DataFrame = musicDataDFParam
  val sc = context
  val sqlContext = sqc
 
  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)
  val reportBasePath = "/home/acadgild/reports/"
  val currentTimestamp = System.currentTimeMillis().toString

  
  def analyze() = {
    import sqlContext.implicits._
    musicDataDF.registerTempTable("MusicDataDetailed")
    log.info("Before calling getTo10Stations()")
  //  getTop10Stations()
    log.info("Before calling getMusicDurtionByUser()")
    getMusicDurtionByUserType()
    log.info("Before calling getTo10ConnectedArtists()")
    getTop10ConnectedArtists()
    log.info("Before calling  getTo10SongsHavignMaximumRevenue()")
  //  getTo10SongsHavignMaximumRevenue()
    log.info("Before calling  getTo10UnsubscribedUsers()")
    getTop10UnsubscribedUsers()
    
  }
  
  def getTop10Stations() {
 //   val sqlContext = new SQLContext(sc)
 //   import sqlContext.implicits._
    log.info(" Top 10 Music Stations where maximum numbers of songs played which are liked by unique users")
  //  musicDataDF.registerTempTable("MusicDataDetailed")
    sqlContext.sql("SELECT Station_id, User_id, count(*) AS music_count FROM MusicDataDetailed "
                   + " WHERE Like='1' AND isValid='1' GROUP BY User_id")
                    .registerTempTable("MusicCountByStation")
    sqlContext.sql("SELECT Station_id, User_id, CASE WHEN music_count> 1 THEN 1 ELSE music_count END "
                   + " AS unique_music_count FROM MusicCountByUser")
                    .registerTempTable("UniqueMusicCountByStation")  
    val df = sqlContext.sql("SELECT Station_id, sum(unique_music_count) AS total_music_count "
                   + " FROM UniqueMusicCountByStation GROUP BY Station_id ORDER BY total_music_count "
                   + " DESC LIMIT 10 ")
     df.show()               
     df.write.format("com.databricks.spark.csv").option("header", "true").save(reportBasePath + "/Top10Stations_" + currentTimestamp)
    
    // musicDataDF.write.text(reportBasePath + "/Top10Stations_" + currentTimestamp)
                   
                   
                   
   
  }
  
  
   def getMusicDurtionByUserType() {
   //   val sqlContext = new SQLContext(sc)
    // import sqlContext.implicits._
    log.info(" Total duration of Songs played by Subscibed and Unsubsribed Users")
  //  musicDataDF.registerTempTable("MusicDataDetailed")
    sqlContext.sql("SELECT CASE WHEN subscribed='1' THEN 'Subscribed' ELSE 'Unsubscribed' END AS User_type, (End_ts -Start_ts) AS duration  "
                   + " FROM MusicDataDetailed WHERE isValid='1'")
                    .registerTempTable("UserTypeDuration")
    val df =sqlContext.sql("SELECT User_type, SUM(duration) AS total_duration_milliseconds FROM UserTypeDuration "
                   + " GROUP BY User_type ORDER BY total_duration_milliseconds DESC")
     df.show()
                    
      df.write.format("com.databricks.spark.csv").option("header", "true").save(reportBasePath + "/MusicDurationByUserType_" + currentTimestamp)               
                    
//    musicDataDF.write.text(reportBasePath + "/MusicDurationByUserType_" + currentTimestamp)
   
  }
   
   
  def getTop10ConnectedArtists() {
   //  val sqlContext = new SQLContext(sc)
   //  import sqlContext.implicits._
    log.info(" Top 10 Connected Artists")
 //   musicDataDF.registerTempTable("MusicDataDetailedDetailed")
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
     df.write.format("com.databricks.spark.csv").option("header", "true").save(reportBasePath + "/Top10ConnectedArtists_" + currentTimestamp)              
  //   musicDataDF.write.text(reportBasePath + "/Top10ConnectedArtists_" + currentTimestamp)              
   
  } 
  
  def getTop10SongsHavignMaximumRevenue() {
   //  val sqlContext = new SQLContext(sc)
   // import sqlContext.implicits._
    log.info(" Top 10 Songs Having maximum revenue")
 //   musicDataDF.registerTempTable("MusicDataDetailed")
    
    
    sqlContext.sql("SELECT Song_id, (End_ts -Start_ts) AS duration FROM MusicDataDetailed "
                   + " WHERE (Like='1' OR Song_end_type =0) AND isValid='1' ")
                    .registerTempTable("SongDuration")
    val df = sqlContext.sql("SELECT Song_Id, SUM(duration) AS total_duration_milliseconds FROM SongDuration "
                   + " GROUP BY Song_Id ORDER BY total_duration_milliseconds DESC LIMIT 10")
    df.show()
     df.write.format("com.databricks.spark.csv").option("header", "true").save(reportBasePath + "/Top10SongsHavignMaximumRevenue_" + currentTimestamp)              
 //   musicDataDF.write.text(reportBasePath + "/Top10SongsHavignMaximumRevenue_" + currentTimestamp)                  
    
  }
  
   def getTop10UnsubscribedUsers() {
   // val sqlContext = new SQLContext(sc)
 //   import sqlContext.implicits._
    log.info(" Top 10 Unsubscribed Users")
 //   musicDataDF.registerTempTable("MusicDataDetailed")
    
    
    sqlContext.sql("SELECT User_id, (End_ts -Start_ts) AS duration FROM MusicDataDetailed "
                   + " WHERE subscribed='0' OR Song_end_type =0 ")
                    .registerTempTable("SongDuration")
    val df = sqlContext.sql("SELECT User_id, SUM(duration) AS total_duration_milliseconds FROM SongDuration "
                   + " GROUP BY User_id ORDER BY total_duration_milliseconds DESC LIMIT 10")
                    
     df.show()   
     df.write.format("com.databricks.spark.csv").option("header", "true").save(reportBasePath + "/tTop10UnsubscribedUsers_" + currentTimestamp)
  //   musicDataDF.write.text(reportBasePath + "/tTop10UnsubscribedUsers_" + currentTimestamp)                  
    
  }
   
   
  
  
}