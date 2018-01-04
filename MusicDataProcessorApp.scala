package final_project
import org.apache.log4j.{ Level, LogManager, PropertyConfigurator }
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.HashMap


object MusicDataProcessorApp {
  def main(args: Array[String]) {
    
  
    val web_file_path = "/data/web/file-1.xml"
    val mobile_file_path = "file:///data/mob/file.txt"
    
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)

    val conf = new SparkConf().setAppName("MusicDataProcessApp").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    sc.setLogLevel("ERROR")
    
    val populateMusicDataMap = new MusicDataPopulateMapFromLookupTables(sc)
       
    val stndIdGeoCdMap:Map[String, String] = populateMusicDataMap.getStationIdGeoCdMap()
    val broadcastStndIdGeoCdMap = sc.broadcast(stndIdGeoCdMap)                     
                      
    val songArtistMap =  populateMusicDataMap.getSongArtistMap()
    val broadcastSongArtistMap = sc.broadcast(songArtistMap) 
    
    val userArtistMap = populateMusicDataMap.getUserArtistMap()
    val broadcastUserArtistMap = sc.broadcast(userArtistMap) 
 
    val userSubscription:Map[String, (Long, Long)] = populateMusicDataMap.getUserSubscriptionMap()
    val broadcastuserSubscription = sc.broadcast(userSubscription)                          
                           

    val webMusicDataProcessor = new WebMusicDataProcessor(web_file_path, sc, sqlContext )
    
    log.info("Before calling webMusicDataProcessor.processData()")
    val webDataFrame:DataFrame = webMusicDataProcessor.processData()
    log.info("After calling webMusicDataProcessor.processData()")
    
     val mobileMusicDataProcessor = new MobileMusicDataProcessor(mobile_file_path, sc, sqlContext)
    
    log.info("Before calling mobileMusicDataProcessor.processData()")
    val mobileDataFrame:DataFrame = mobileMusicDataProcessor.processData()
    log.info("After calling webMusicDataProcessor.processData()")
    
    val allDataFrame:DataFrame = webDataFrame.unionAll(mobileDataFrame)
    log.info("allDataFrame count =" + allDataFrame.count)
    
 //   val musicDataEnricher = new MusicDataEnricher(allDataFrame, stndIdGeoCdMap, broadcastSongArtistMap, broadcastUserArtistMap, broadcastuserSubscription) 
     val musicDataEnricher = new MusicDataEnricher(allDataFrame, broadcastStndIdGeoCdMap, broadcastSongArtistMap, broadcastUserArtistMap, broadcastuserSubscription) 
    
    val enrichedAllDataFrame:DataFrame = musicDataEnricher.enrichData()
     log.info("Showing dataframe after enrichment")
     enrichedAllDataFrame.show(100)
      enrichedAllDataFrame.registerTempTable("DetailedMusicData")
     
     val musicDataAnalyzer = new MusicDataAnalyzer(sc, sqlContext, enrichedAllDataFrame)
     musicDataAnalyzer.analyze()
  }
  
  


}