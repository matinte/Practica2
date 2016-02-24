package com.pragsis.master.practicaALS

import java.io.FileWriter
import java.nio.file.{Paths, Files}

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import java.io.File
import scala.collection.Map


object ALSMusicToGo{

  // paths
  val LOCAL_PATH="/home/miguel/Escritorio/Practica2/"
  val INPUT_TEST = LOCAL_PATH+"ratings/test"
  val INPUT_TRAIN = LOCAL_PATH+"ratings/training"
  val INPUT_VALIDATION = LOCAL_PATH+"ratings/validation"
  val MODEL_PATH = LOCAL_PATH+"model"
  val STREAMING_DATA_FILE = LOCAL_PATH+"streaming/data.txt"
  
  // global variables
  val MAX_PETICIONES = 5
  val TIME =1
  val NUMBER_CORES = 8
  var RANK = 0

  /**
    * Main
    * @param args
    */
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("ALSMusicToGo")
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)

    LogManager.getRootLogger.setLevel(Level.ERROR)

    // Declaracion acumulador
    val accum = sc.accumulator(0,"ContadorPeticiones")

    // calculamos ratins de dataset test 
    val datosReproducciones = ProcessFile.calculateRating(sc,INPUT_TEST)

    // mapa idgrupo:nombregrupo
    var datosGrupos = datosReproducciones.map(dato=>(dato.idGrupo,dato.nombreGrupo)).reduceByKey((a:String,b:String)=>b).collectAsMap()

    // comparamos modelos
    val bestModel = CompareModels.compareModel(sc, INPUT_TRAIN, INPUT_VALIDATION, INPUT_TEST) 
    
    // primera carga del modelo
    RANK = bestModel.rank // se almacena el rank del mejor modelo
    var storedModel = getModel(sc,false,false,RANK)
    
    println("Recomendar grupos para usuario: b2d47bbf36f17961ccea17d40e98c7b3c6c1fcc2")
    recommendArtists(storedModel,"b2d47bbf36f17961ccea17d40e98c7b3c6c1fcc2",datosGrupos)  
    
    println("----------------------------------")    
    println("Streaming...")
    var ssc = defineStreaming(sc,accum,storedModel,datosGrupos)
    println("Nuevo contexto stream cargado")
    println("----------------------------------")
    ssc.start()

    while(true){
      if(accum.value >= MAX_PETICIONES){

        accum.setValue(0)
        ssc.stop(false,true)

        // generar el nuevo modelo
        storedModel = getModel(sc,true,true,RANK)

        println("Generando nuevo diccionario de grupos")
        val reps = ProcessFile.calculateRating(sc,INPUT_TEST)
        datosGrupos = datosReproducciones.map(dato=>(dato.idGrupo,dato.nombreGrupo)).reduceByKey((a:String,b:String)=>b).collectAsMap()

        println("Borrando datos streaming anterior")
        var file  = new File(STREAMING_DATA_FILE)
        file.delete()

        ssc = defineStreaming(sc,accum,storedModel,datosGrupos)
        println("Nuevo contexto stream cargado")
        println("----------------------------------")
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
    var lista = List[String]()

    lines.foreachRDD(rdd=>{
      val lineas = rdd.collect().toIterator
      if(rdd.count()>0){
        lineas.foreach(linea=>{
          // Aumentar acumulador
          accum+=1

          var campos = linea.split("::")

          if(! lista.contains(campos(0))){
            try{
              lista = campos(0) :: lista
              recommendArtists(modelo,campos(0),datosGrupos)
            }catch {
              case e: Exception => {
                println("No hay recomendaciones para el usuario "+campos(0))
                println("----------------------------------")
              }
            }
          }else{
            println("No hay recomendaciones actualizadas para el usuario "+campos(0))
            println("----------------------------------")
          }


          // Escribir el fichero de streaming
          try{
            val cadena=campos(0)+":##:"+campos(0).hashCode+":##:"+campos(3)+":##:"+campos(3).hashCode+":##:"+1
            val writer = new FileWriter(STREAMING_DATA_FILE,true)
            writer.write(cadena+"\n")
            writer.close()
          }catch {
            case e: Exception => {
              println("Error escribiendo fichero de streaming")
              println("----------------------------------")
            }
          }

        })
      }
    })
    ssc
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

    println("Recomendaciones para usuario "+userid+":")
    listArtistsRecom.foreach(println)
    println("----------------------------------")

    // Escritura a Hbase
    listArtistsRecom.foreach({case (artist,rate)=> HBaseManager.saveToHBase(userid,artist,rate)} )

  }


  /**
    * Generacion modelo de prediccion
    * @param sc
    * @param overrideModel
    * @param isStreaming
    * @return
    */
  def getModel(sc: SparkContext, overrideModel: Boolean, isStreaming: Boolean, rank: Integer):MatrixFactorizationModel={
    if (Files.exists(Paths.get(MODEL_PATH)) && !overrideModel){
      println("Cargando modelo de disco...")
      MatrixFactorizationModel.load(sc, MODEL_PATH)
    } else {
      println("Generando modelo...")
      val test_dataset = ProcessFile.getCompleteData(sc,isStreaming,INPUT_TEST,STREAMING_DATA_FILE)
      val test_rating = ProcessFile.calculateRating(sc,INPUT_TEST)
      CompareModels.calculateModel(sc,MODEL_PATH,test_rating,rank)
    }
  }

}
