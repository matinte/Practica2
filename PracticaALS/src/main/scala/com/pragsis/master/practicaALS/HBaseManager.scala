package com.pragsis.master.practicaALS

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.conf.Configuration

object HBaseManager {
  
  val TABLE_NAME = "Top10ArtistForUser"
  
  def connectDatabase():Configuration={
    //hbase config
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "quickstart.cloudera")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.setInt("timeout", 120000)
    hbaseConf
  }
  
  def getTable():Table={
    val hbaseConf = connectDatabase()
    // hbase database connection
    var hbaseAdmin = new HBaseAdmin(hbaseConf)
    if (!hbaseAdmin.tableExists(TABLE_NAME)) {
      createTable(hbaseAdmin,hbaseConf)
    } else {
      val connection = ConnectionFactory.createConnection(hbaseConf)
      connection.getTable(TableName.valueOf(TABLE_NAME))
    }
  }
  
  def saveToHBase(userid: String, artist: String, rate: Double):Unit={
    val table = getTable()
    val put = new Put(Bytes.toBytes(userid))
    put.addColumn(Bytes.toBytes("artist"), Bytes.toBytes(artist), Bytes.toBytes(rate)) 
    table.put(put);  
  }

  def createTable(hbaseAdmin: HBaseAdmin, hbaseConf: Configuration):Table={
    // table description
    val tableDesc = new HTableDescriptor(Bytes.toBytes(TABLE_NAME))
    // add userid CF
    val userColumnFamilyDesc = new HColumnDescriptor(Bytes.toBytes("userid"))
    tableDesc.addFamily(userColumnFamilyDesc)
    // add artist CF
    val artistsColumnFamilyDesc = new HColumnDescriptor(Bytes.toBytes("artist"))
    tableDesc.addFamily(artistsColumnFamilyDesc)
    hbaseAdmin.createTable(tableDesc)
    // return table
    val connection = ConnectionFactory.createConnection(hbaseConf)
    connection.getTable(TableName.valueOf(TABLE_NAME))
   }
  
}  


