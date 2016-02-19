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

object HBaseManager {
  
  def connectDatabase():Table={
    //hbase config
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "quickstart.cloudera")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.setInt("timeout", 120000)
    // hbase database connection
    val connection = ConnectionFactory.createConnection(hbaseConf)
    val table = connection.getTable(TableName.valueOf("recommendedArtist"))
    table
  }
  
  def saveToHBase(userid: String, artist: String, rate: Double):Unit={
    val table = connectDatabase()
    val put = new Put(Bytes.toBytes(userid))
    put.addColumn(Bytes.toBytes("artist"), Bytes.toBytes(artist), Bytes.toBytes(rate)) 
    table.put(put);  
  }

  //********************************************************
  // test HBase: connect, create, put, get
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HbaseSample").setMaster("local[4]")
    val sc = new SparkContext(conf)

    //hbase config
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "quickstart.cloudera")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.setInt("timeout", 120000)
    
    // hbase database connection
    val connection = ConnectionFactory.createConnection(hbaseConf)
    val table = connection.getTable(TableName.valueOf("recommendedArtist"))

    // create database
    val tableDesc = new HTableDescriptor(Bytes.toBytes("recommendedArtist"))
    // add userid CF
    val userColumnFamilyDesc = new HColumnDescriptor(Bytes.toBytes("userid"))
    tableDesc.addFamily(userColumnFamilyDesc)
    // add artist CF
    val artistsColumnFamilyDesc = new HColumnDescriptor(Bytes.toBytes("artist"))
    tableDesc.addFamily(artistsColumnFamilyDesc)
    val admin = new HBaseAdmin(hbaseConf)
    //admin.createTable(tableDesc)
    
    // put row
    val userid = "001"
    val put = new Put(Bytes.toBytes(userid))
    //Bytes.toBytes("cf"), Bytes.toBytes("a"), Bytes.toBytes(record)
    put.addColumn(Bytes.toBytes("artist"), Bytes.toBytes("David Bowie"), Bytes.toBytes("10")) 
    put.addColumn(Bytes.toBytes("artist"), Bytes.toBytes("Red Hot Chili Peppers"), Bytes.toBytes("5"))
    put.addColumn(Bytes.toBytes("artist"), Bytes.toBytes("Nirvana"), Bytes.toBytes("15"))
    table.put(put);
    
    // get row  
    val get = new Get(Bytes.toBytes(userid))
    val row = table.get(get)
    val value = row.getValue(Bytes.toBytes("artist"), Bytes.toBytes("Nirvana"))
    val valueStr = Bytes.toString(value)
    val scanner = table.getScanner(new Scan());
    
    // print row
    println(userid + ": " + valueStr)
  }
}  