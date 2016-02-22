package com.pragsis.master.practicaALS

import java.io.FileWriter
import java.nio.file.{Paths, Files}

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

import scala.collection.Map

object PruebaStreaming{

  val MAX_PETICIONES = 5
  val TIME =1
  val LOCAL_PATH="/home/david/Pragsis/Practica2/"
  val NUMBER_CORES = 8
  val INPUT_TEST = LOCAL_PATH+"ratings/test"
  val INPUT_TRAINING = LOCAL_PATH+"ratings/training"
  val MODEL_PATH = LOCAL_PATH+"model"
  val STREAMING_DATA_FILE = LOCAL_PATH+"streaming/data.txt"

  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("CompareModels")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    LogManager.getRootLogger.setLevel(Level.ERROR)


    val accum = sc.accumulator(0,"ContadorPeticiones")

    val datosReproducciones = CompareModels.loadDatasetDatos(sc,INPUT_TEST)
    println("Total number of ratings: " + datosReproducciones.count())
    println("Total number of groups rated: " + datosReproducciones.map(_.idGrupo).distinct().count())
    println("Total number of users who rated artists: " + datosReproducciones.map(_.idUsuario).distinct().count())

    // mapa idgrupo:nombregrupo
    val datosGrupos = datosReproducciones.map(dato=>(dato.idGrupo,dato.nombreGrupo)).reduceByKey((a:String,b:String)=>b).collectAsMap()

    // primera carga del modelo
    var storedModel = getModel(sc,false)

    var ssc = defineStreaming(sc,accum,storedModel,datosGrupos)
    ssc.start()

    while(true){
      if(accum.value >= MAX_PETICIONES){
        accum.setValue(0)
        ssc.stop(false,true)
        // generar el nuevo modelo
        storedModel = getModel(sc,false)
        ssc = defineStreaming(sc,accum,storedModel,datosGrupos)
        ssc.start()
      }
    }
  }

  /**
    * Definicion del comportamiento del stream
    *
    * @param sc
    * @param accum
    * @param modelo
    * @return
    */
  def defineStreaming(sc: SparkContext, accum:Accumulator[Int], modelo:MatrixFactorizationModel,datosGrupos: Map[Int, String]): StreamingContext ={
    val ssc = new StreamingContext(sc, Seconds(TIME))
    val lines = ssc.socketTextStream("localhost", 9999)

    lines.foreachRDD(rdd=>{
      val lineas = rdd.collect().toIterator
      if(rdd.count()>0){
        lineas.foreach(linea=>{
          // Aumentar acumulador
          accum+=1

          var campos = linea.split("::")
          try{
            recommendArtists(modelo,campos(0),datosGrupos)
          }catch {
            case e: Exception => {
              println("No hay recomendaciones para el usuario "+campos(0))
              println("----------------------------------")
            }
          }

          // Escribir el fichero de streaming
          val cadena=campos(0)+":##:"+campos(0).hashCode+":##:"+campos(3)+":##:"+campos(3).hashCode+":##:"+1
          val writer = new FileWriter(STREAMING_DATA_FILE,true)
          try{
            writer.write(cadena+"\n")
            writer.close()
          }

        })
      }
    })
    ssc
  }

  def getModel(sc: SparkContext, overrideModel: Boolean):MatrixFactorizationModel={
    if (Files.exists(Paths.get(MODEL_PATH)) && !overrideModel){
      println("Cargando modelo de disco...")
      MatrixFactorizationModel.load(sc, MODEL_PATH)
    } else {
      println("Generando modelo")
      CompareModels.calculateModel(sc,MODEL_PATH,INPUT_TEST)
    }
  }

  /**
    * Recomienda grupos para un determinado usuario
    *
    * @param modelo
    * @param userid
    * @param datosGrupos
    */
  def recommendArtists(modelo:MatrixFactorizationModel,userid: String, datosGrupos: Map[Int,String]){
    // Artistas recomendados
    val artistsRecommended = modelo.recommendProducts(userid.hashCode,10)

    // Ordenar y obtenernombre del grupo
    val listArtistsRecom = artistsRecommended.sortBy(- _.rating).map(r=>(datosGrupos(r.product),r.rating))

    println("List recommended for "+userid)
    listArtistsRecom.foreach(println)
    println("----------------------------------")

    // Escritura a Hbase
    //listArtistsRecom.foreach({case (artist,rate)=> HBaseManager.saveToHBase(userid,artist,rate)} )

  }

}
