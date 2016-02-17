package com.pragsis.master.practicaALS

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.Rating

object ProcessFile {

  def main(args: Array[String]): Unit = {
    // configure Spark Context
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Music To Go")
    val sc = new SparkContext(sparkConf)
    val MAX_EXPLICIT_RATING = 5.0
    
    val file = sc.textFile("/home/cloudera/Desktop/Practica1/dataset_practico1/lastfm-dataset-360K/usersha1-artmbid-artname-plays.tsv")

    val testFile = file.randomSplit(Array(0.01,0.99),0L)

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
      val rate = (data._2._1._2*MAX_EXPLICIT_RATING)/data._2._2
      (data._1,(data._2._1._1,rate.toDouble))
    }).sortByKey(false).map(tupla=>{
      tupla._1+";"+tupla._2._1+";"+tupla._2._2
      //Rating(tupla._1.toInt, tupla._2._1.toInt, tupla._2._2.toDouble)
    })

    ratings.saveAsTextFile("/home/cloudera/Desktop/Practica2/ratings")



  }
  
}
