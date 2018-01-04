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


class MusicDataPopulateMapFromLookupTables(context: SparkContext) {
  val sc:SparkContext = context  
  val hconf = HBaseConfiguration.create
  val admin = new HBaseAdmin(hconf)
  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)
    
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
  
  def getStationIdGeoCdMap() = {
    val tablename = "StndIdGeoCd"
    hconf.set(TableInputFormat.INPUT_TABLE, tablename)
    getCommonLookupMap(tablename)
    
    
  }
  
  def getSongArtistMap() = {
    val tablename = "SongArtist"
    hconf.set(TableInputFormat.INPUT_TABLE, tablename)
    getCommonLookupMap(tablename)    
    
  }
    
  def getUserArtistMap() = {
    val tablename = "UserArtist"
    hconf.set(TableInputFormat.INPUT_TABLE, tablename)
    getCommonLookupMap(tablename)   
    
  }
  
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



