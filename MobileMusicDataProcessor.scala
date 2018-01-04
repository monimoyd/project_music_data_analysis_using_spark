package final_project

import org.apache.spark._
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.log4j.{ Level, LogManager, PropertyConfigurator }
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable.HashMap

case class MusicData(User_id:String, Songs_id:String, Artist_id:String, Timestamp:Long,
      Start_ts:Long, End_ts:Long, Geo_cd:String, Station_id:String, Song_end_type:String,
      Likes:String, Dislikes:String)

class MobileMusicDataProcessor(param: String, context: SparkContext, sqc:SQLContext) extends Serializable {
  val filePath: String = param
  val sc: SparkContext = context
  val sqlContext:SQLContext = sqc
  
  
  def processData(): DataFrame = {
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    
    import sqlContext.implicits._

    val recordRDD = sc.textFile(filePath)
    val recordFieldsRDD = recordRDD.map(x => x.split(",")).filter(x=> x.length ==11)
   
    
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