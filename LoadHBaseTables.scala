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

object LoadHBaseTables {
  def main(args: Array[String]) {
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    
    val filePath = args(0)
    val tablename = args(1)
    val columnFamilyField = args(2)
    val columnFamilyFieldList = columnFamilyField.split(",")
    val columnFamilyFieldListLength = columnFamilyFieldList.length

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
      tableDescription.addFamily(new HColumnDescriptor(columnFamilyFieldList(0).getBytes()))
      admin.createTable(tableDescription)
      if (admin.isTableAvailable(tablename)) {
         log.info("Table " + tablename + " is created successfully")
      }
    } else {
      log.warn("Table " + tablename + " already exists")
    }
     
   
    val table = new HTable(hconf, tablename)
    
    val file = sc.textFile("file://" + filePath)
    
    var records1:Array[(String,String)] = null
    var records2:Array[(String,String,String)] = null
    if (columnFamilyFieldListLength == 2) {
        records1 = file.map(_.split(",")).map(x => (x(0), x(1))).collect
    } else if (columnFamilyFieldListLength == 4) {
        records2 = file.map(_.split(",")).map(x => (x(0), x(1), x(2))).collect
    }
    
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
