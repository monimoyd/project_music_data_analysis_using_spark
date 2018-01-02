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
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.log4j.{Level, LogManager, PropertyConfigurator}

object LoadHBaseTables {
  def main(args: Array[String]) {
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    
    val tablename = "TempUserArtist"

    val conf = new SparkConf().setAppName("LoadHbaseTable").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)
    val hconf = HBaseConfiguration.create
    hconf.set(TableInputFormat.INPUT_TABLE, tablename)
    val admin = new HBaseAdmin(hconf)
    
    
    
    
      
    if (!admin.isTableAvailable(tablename)) {
      log.info("Creating table " + tablename)
      val tableDescription = new HTableDescriptor(tablename)
      tableDescription.addFamily(new HColumnDescriptor("Artist".getBytes()))
      admin.createTable(tableDescription)
      if (admin.isTableAvailable(tablename)) {
         log.info("Table " + tablename + " is created successfully")
      }
    } else {
      log.warn("Table " + tablename + " already exists")
    }
        
    
   
   
    val table = new HTable(hconf, tablename)
    
    val file = sc.textFile("file:///home/acadgild/final_project/user-artist.txt")
    val records = file.map(_.split(",")).map(x => (x(0), x(1))).collect
    
    for(record <- records) {
      var p = new Put(new String(record._1).getBytes())
      p.add("Artist".getBytes(), "names".getBytes(), new String(record._2).getBytes())
      table.put(p)
    }

    table.flushCommits()

   
  }

}
