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

case class CustomException(message:String) extends Exception(message)

class WebMusicDataProcessor(param: String, context: SparkContext, sqc: SQLContext) extends Serializable {
  val filePath: String = param
  val sc: SparkContext = context
  val sqlContext:SQLContext = sqc
  


  def processData(): DataFrame = {
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
  //  val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val recordListBuffer = new ListBuffer[(String, String, String, Long, Long, Long, String, String, String, String, String)]()
        
    val xml = XML.loadFile(filePath)
        

    for (tag <- xml.child) {
      val userId = (tag \ "user_id").text
      val songId = (tag \ "song_id").text
      var artistId = (tag \ "artist_id").text
      
     // if (artistId.equals("")) {
     //   artistId = broadcastSongArtistMap.value.get(songId).getOrElse("Invalid")
    //  }
      
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
      
      //if (geoCd.equals("")) {
      //  geoCd = broadcastStndIdGeoCdMap.value.get(stationId).getOrElse("Invalid")
     // }
      
      val songEndType = (tag \ "song_end_type").text
      var like = (tag \ "like").text
      if (like.equals("")) like = "0"
      var dislike = (tag \ "dislike").text
      if (dislike.equals("")) dislike = "0"
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
    val recordList = recordListBuffer.toList
         
    val recordRDD = sc.parallelize(recordList)
    val recordDF = recordRDD.toDF("User_id", "Song_id", "Artist_id", "Timestamp",
      "Start_ts", "End_ts", "Geo_cd", "Station_id", "Song_end_type",
       "Like", "Dislike")
       
    log.info("Number of records =" + recordDF.count)
    log.info("Showing records for Web Music Data ")
    recordDF.show

    return recordDF
    
  }


}