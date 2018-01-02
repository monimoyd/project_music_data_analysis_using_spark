package final_project
import org.apache.log4j.{ Level, LogManager, PropertyConfigurator }
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.HashMap


object MusicDataProcessorApp {
  def main(args: Array[String]) {
    
  
    val web_file_path = "/home/acadgild/final_project/file-1.xml"
    val mobile_file_path = "file:///home/acadgild/final_project/file.txt"
    
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)

    val conf = new SparkConf().setAppName("MusicDataProcessApp").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    sc.setLogLevel("ERROR")
    
      val stndIdGeoCdMap:HashMap[String, String] = HashMap("ST400" -> "A",
                          "ST401" -> "AU",
                          "ST402" -> "AP",
                          "ST403" -> "J",
                          "ST404" -> "E",
                          "ST405" -> "A",
                          "ST406" -> "AU",
                           "ST407" -> "AP",
                           "ST408" -> "E",
                           "ST409" -> "E",
                           "ST410" -> "A",
                           "ST411"-> "A",
                           "ST412" -> "AP",
                           "ST413" -> "J",
                           "ST41" -> "E"
                      )
                      
 val broadcastStndIdGeoCdMap = sc.broadcast(stndIdGeoCdMap)                     
                      
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
 
 
val userArtistMap = Map( "U100" -> "A300&A301&A302",
                         "U101" ->  "A301&A302",                         
                         "U102" -> "A302",
                         "U103" -> "A303&A301&A302",
                         "U104" -> "A304&A301",
                         "U105" -> "A305&A301&A302",
                         "U106" -> "A301&A302",
                         "U107" -> "A302",
                          "U108" -> "A300&A303&A304",
                          "U109"-> "A301&A303",
                          "U110" -> "A302&A301",
                          "U111" -> "A303&A301",
                           "U112" -> "A304&A301",
                           "U113" -> "A305&A302",
                           "U114"-> "A300&A301&A302"
                           )
 val broadcastUserArtistMap = sc.broadcast(userArtistMap) 
 
 val userSubscription:Map[String, (Long, Long)] = Map(
                          "U100" -> (1465230523,1465130523),
                           "U101" ->  (1465230523,1475130523),
                           "U102" -> (1465230523,1475130523),
                           "U103" -> (1465230523,1475130523),
                           "U104"-> (1465230523,1475130523),
                           "U105"-> (1465230523,1475130523),
                           "U106" -> (1465230523,1485130523),
                           "U107" -> (1465230523,1455130523),
                           "U108" -> (1465230523,1465230623),
                           "U109"->  (1465230523,1475130523),
                           "U110"-> (1465230523,1475130523),
                           "U111" -> (1465230523,1475130523),
                           "U112"-> (1465230523,1475130523),
                           "U113"-> (1465230523,1485130523),
                           "U114"-> (1465230523,1468130523)
                           )
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