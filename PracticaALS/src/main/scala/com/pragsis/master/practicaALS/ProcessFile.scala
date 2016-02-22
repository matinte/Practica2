package com.pragsis.master.practicaALS

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import java.lang.Boolean
import com.pragsis.master.practicaALS.CompareModels.DatosUserPlays
import com.pragsis.master.practicaALS.CompareModels.DatosUsuario

object ProcessFile {
  
    val MAX_EXPLICIT_RATING = 5.0
    val LOCAL_PATH="/home/cloudera/Desktop/Practica2/"
    var INPUT_FILE=LOCAL_PATH+"usersha1-artmbid-artname-plays.tsv"
    var INPUT_STREAMING = LOCAL_PATH+"ratings/streaming"
    var OUTPUT_FILE_TEST=LOCAL_PATH+"ratings/test"
    var OUTPUT_FILE_TRAINING=LOCAL_PATH+"ratings/training"
    var OUTPUT_FILE_VALIDATION=LOCAL_PATH+"ratings/validation"
    
  def main(args: Array[String]): Unit = {
    
    // configure Spark Context
    val sparkConf = new SparkConf().setAppName("MusicToGo")

    // Se comprueba ejecucion en local
    if(args.length == 0 ){
      sparkConf.setMaster("local[4]")
    }else{
      INPUT_FILE = args(0)
      OUTPUT_FILE_TRAINING = args(1)
      OUTPUT_FILE_VALIDATION = args(2)
      OUTPUT_FILE_TEST = args(3)
    }

    val sc = new SparkContext(sparkConf)

    // join files
    val completeData = getCompleteData(sc,false)
    
    // calculamos ratings
    //val ratings = calculateRating(sc)

    // split del fichero total con user,user.id,group,group.id,plays
    val partes = completeData.randomSplit(Array(0.06,0.02,0.02),0L)
    // Escritura a fichero
    partes(0).saveAsTextFile(OUTPUT_FILE_TRAINING)
    partes(1).saveAsTextFile(OUTPUT_FILE_VALIDATION)
    partes(2).saveAsTextFile(OUTPUT_FILE_TEST)
  }
  
  def getCompleteData(sc: SparkContext, concatStreaming: Boolean):RDD[CompareModels.DatosUserPlays]={
    
    // lectura de fichero
    val file = sc.textFile(INPUT_FILE)
    
      //val data = file.map(linea=>((linea.split('\t')(0),linea.split('\t')(2)),Integer.parseInt(linea.split('\t')(3)))).reduceByKey((x,v)=>x+v);
    
    val data = file.map(linea=>{
      val campos = linea.split('\t')
      val plays = Integer.parseInt(campos(3).trim.replaceFirst("^0+(?!$)",""))
      val user = campos(0).trim
      val artist = campos(2).trim
      ((user,user.hashCode(),artist,artist.hashCode()),plays)})
      //user+":##:"+user.hashCode+":##:"+artist+":##:"+artist.hashCode+":##:"+plays
    
    if (concatStreaming) {
      // lectura de fichero
      val strfile = sc.textFile(INPUT_STREAMING)
      val dataStr = strfile.map(linea=>{
        val campos = linea.split("::")
        val plays = 1
        val user = campos(0).trim
        val artist = campos(3).trim
        val artistid = campos(2).trim
      ((user,user.hashCode(),artist,artist.hashCode()),plays)
      }).reduceByKey((k,v)=>k+v)
      
      // join
      data.join(dataStr).map(tupla=>(tupla._1,tupla._2._1+tupla._2._2))
    } else {
      None 
    }
    // return all data
    val dataFinal = data.map(tupla=>DatosUserPlays(tupla._1._1,tupla._1._2,tupla._1._3,tupla._1._4,tupla._2))
    val dataString = dataFinal.map(datos=>datos.idUsuario+"\t"+datos.idGrupo+"\t"+datos.nombreGrupo+"\t"+datos.plays)
    dataString.saveAsTextFile(OUTPUT_FILE_TEST)
    dataFinal
  }
  
  def calculateRating(sc: SparkContext):RDD[CompareModels.DatosUsuario]={
    
    // lectura de fichero
    val file = sc.textFile(OUTPUT_FILE_TEST)
    // Obtencion de rdd con los usurios, grupos y numeros de reproducciones
    val usuarios = file.map(linea=>{
      val campos = linea.split('\t')
      val plays = Integer.parseInt(campos(3).trim.replaceFirst("^0+(?!$)",""))
      (campos(0).trim,(campos(2).trim,plays))
    })
    
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
    
    // join
    val total = usuarios.join(maximos)

    // calculo de los ratings por usuario
    val ratings = total.map(data=>{
      val rate = (data._2._1._2*MAX_EXPLICIT_RATING)/data._2._2
      (data._1,(data._2._1._1,rate.toDouble))
    }).sortByKey(false).map(tupla=>{
      // Se emite nombreUsurio;idUsuario;nombreGrupo;idGrupo;nota
      //tupla._1+":##:"+tupla._1.hashCode+":##:"+tupla._2._1+":##:"+tupla._2._1.hashCode+":##:"+tupla._2._2
      DatosUsuario(tupla._1,tupla._1.hashCode,tupla._2._1,tupla._2._1.hashCode,tupla._2._2)
    })
    
    // trazas
    println("Total number of ratings: " + ratings.count())
    
    // devolvemos RDD total con ratings
    ratings
  }

}