package com.pragsis.master.practicaALS

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

object ProcessFile {

  def main(args: Array[String]): Unit = {
    // configure Spark COntext
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Music To Go")
    val sc = new SparkContext(sparkConf)

    val file = sc.textFile("/home/cloudera/Downloads/dataset_practico1/lastfm-dataset-360K/usersha1-artmbid-artname-plays.tsv")

    val testFile = file.randomSplit(Array(0.1,0.9),0L)

    val maximos = testFile(0).map(linea => {
      val campos = linea.split('\t')
      try{
        val plays = Integer.parseInt(campos(3).replaceFirst("^0+(?!$)",""))
        (campos(0),plays)
      }catch{
        case e: Exception => (campos(1),1)
      }

    }).reduceByKey((acc,valor)=>{
      if(valor > acc){
        valor
      }else{
        acc
      }
    })


    val usuarios = testFile(0).map(linea=>{
      val campos = linea.split('\t')
      try{
        val plays = Integer.parseInt(campos(3).replaceFirst("^0+(?!$)",""))
        (campos(0),(campos(2),plays))
      }catch{
        case e: Exception => (campos(1),(campos(2),1))
      }
    })

    val total = usuarios.join(maximos)

    val ratings = total.map(data=>{
      (data._1,(data._2._1._1,(data._2._1._2*5/data._2._2)))
    }).sortByKey(false).map(tupla=>{
      tupla._1+";"+tupla._2._1+";"+tupla._2._2
    })

    ratings.saveAsTextFile("/home/cloudera/practica2/ratings")



  }
  
}
