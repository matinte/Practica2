package com.pragsis.master.practicaALS

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.Rating

object ProcessFile {

  def main(args: Array[String]): Unit = {
    // configure Spark Context

    var INPUT_FILE="/home/david/Pragsis/lastfm-dataset-360K/usersha1-artmbid-artname-plays.tsv"
    var OUTPUT_FILE="/home/david/Pragsis/Practica2/ratings"


    val sparkConf = new SparkConf().setAppName("MusicToGo")

    val MAX_EXPLICIT_RATING = 5.0

    // Se comprueba ejecucion en local
    if(args.length == 0 ){
      sparkConf.setMaster("local[4]")
    }else{
      INPUT_FILE = args(0)
      OUTPUT_FILE = args(1)
    }

    val sc = new SparkContext(sparkConf)

    // lectura de fichero
    val file = sc.textFile(INPUT_FILE)

    // Calculo de los maximos por usuario
    val maximos = file.map(linea => {
      val campos = linea.split('\t')
      val plays = Integer.parseInt(campos(3).trim.replaceFirst("^0+(?!$)",""))
      (campos(0).trim,plays)
    }).reduceByKey((acc,valor)=>{
      if(valor > acc){
        valor
      }else{
        acc
      }
    })


    // Obtencion de rdd con los usurios, grupos y numeros de reproducciones
    val usuarios = file.map(linea=>{
      val campos = linea.split('\t')
      val plays = Integer.parseInt(campos(3).trim.replaceFirst("^0+(?!$)",""))
      (campos(0).trim,(campos(2).trim,plays))
    })

    // join
    val total = usuarios.join(maximos)

    // calculo de los ratings por usuario
    val ratings = total.map(data=>{
      val rate = (data._2._1._2*MAX_EXPLICIT_RATING)/data._2._2
      (data._1,(data._2._1._1,rate.toDouble))
    }).sortByKey(false).map(tupla=>{
      // Se emite nombreUsurio;idUsuario;nombreGrupo;idGrupo;nota
      tupla._1+":##:"+tupla._1.hashCode+":##:"+tupla._2._1+":##:"+tupla._2._1.hashCode+":##:"+tupla._2._2
    })

    // Escritura a fichero
    ratings.saveAsTextFile(OUTPUT_FILE)

  }
  
}
