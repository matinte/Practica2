package com.pragsis.master.practicaALS

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

object PruebaStreaming{

  val TIME = 1

  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("CompareModels")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    LogManager.getRootLogger.setLevel(Level.ERROR)

    println("Recibidas 5 lineas")
    val accum = sc.accumulator(0,"ContadorPeticiones")

    var ssc = defineStreaming(sc,accum)
    ssc.start()

    while(true){
      if(accum.value >= 5){
        accum.setValue(0)
        ssc.stop(false,true)
        ssc = defineStreaming(sc,accum)
        ssc.start()
      }
    }
  }

  def defineStreaming(sc: SparkContext, accum:Accumulator[Int]): StreamingContext ={
    val ssc = new StreamingContext(sc, Seconds(TIME))
    val lines = ssc.socketTextStream("localhost", 9999)

    lines.foreachRDD(rdd=>rdd.foreach(x=>{
      println("Recibida la linea :"+x)
      accum+=1
    }))
    ssc
  }

}
