package final_project

import org.apache.spark._
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.log4j.{ Level, LogManager, PropertyConfigurator }
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable.HashMap

case class MusicData(User_id:String, Song_id:String, Artist_id:String, Timestamp:Long,
      Start_ts:Long, End_ts:Long, Geo_cd:String, Station_id:String, Song_end_type:String,
      Like:String, Dislike:String)

class MobileMusicDataProcessor(param: String, context: SparkContext, sqc:SQLContext) extends Serializable {
  val filePath: String = param
  val sc: SparkContext = context
  val sqlContext:SQLContext = sqc
  
  
  
  val handleNull = (param1:String, param2:String, param3:Broadcast[Map[String, String]] )  =>  if (param1.equals("")) param3.value.get(param2).getOrElse("Invalid") else param1 
  
  def processData(): DataFrame = {
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    
   // val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val recordRDD = sc.textFile(filePath)
    val recordFieldsRDD = recordRDD.map(x => x.split(",")).filter(x=> x.length ==11)
   
    val transformFunc = (x:  Array[String], broadcastStndIdGeoCdMap:Broadcast[Map[String, String]],broadcastSongArtistMap:Broadcast[Map[String, String]] ) =>
       MusicData(x(0),
          x(1), 
       //   if (x(2).equals("")) broadcastSongArtistMap.value.get(x(1)).getOrElse("Invalid") else x(2),
          x(2),
          x(3).toLong, 
          x(4).toLong,
          x(5).toLong,
     //     if (x(6).equals(""))  broadcastStndIdGeoCdMap.value.get(x(7)).getOrElse("Invalid") else  x(6),
          x(6),
          x(7),
          x(8),
          if (x(9).equals("")) "0" else x(9),
          if (x(10).equals("")) "0" else x(10))
      
    
    
 //   val recordDF = recordFieldsRDD.map(x => transformFunc(x, broadcastStndIdGeoCdMap, broadcastSongArtistMap)).toDF
    
    val recordDF = recordFieldsRDD.map(x => MusicData(x(0),
        x(1), 
    //    if (x(2).equals("")) broadcastSongArtistMap.value.get(x(1)).getOrElse("Invalid") else x(2),
        x(2),
  //      broadcastSongArtistMap.value.get(x(1)).getOrElse("Invalid"),
   //     broadcastSongArtistMap.value.get(x(1)),
     //   handleNull(x(2), x(1), broadcastSongArtistMap),
        x(3).toLong, 
        x(4).toLong,
        x(5).toLong,
     //   if (x(6).equals(""))  broadcastStndIdGeoCdMap.value.get(x(7)).getOrElse("Invalid") else  x(6),
        x(6),
        x(7),
        x(8),
        if (x(9).equals("")) "0" else x(9),
        if (x(10).equals("")) "0" else x(10))).toDF
       
        
      
  //   val recordFieldsRDD = recordRDD.map(x => x.split(",")).filter(x => x.length == 11).map(x => (x(0), x(1), x(2),
    //    x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10)))
                          
                          
  //   val recordDF = recordFieldsRDD.toDF("User_id", "Song_id", "Artist_id", "Timestamp",
  //    "Start_ts", "End_ts", "Geo_cd", "Station_id", "Song_end_type",
  //    "Like", "Dislike")

    log.info("Number of records for Mobile Music Data =" + recordDF.count)
    log.info("Showing records for Mobile Music Data ")
    recordDF.show

    return recordDF

  }

}