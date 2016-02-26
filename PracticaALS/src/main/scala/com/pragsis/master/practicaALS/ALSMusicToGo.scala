package com.pragsis.master.practicaALS

import java.io.{File, FileWriter}
import java.nio.file.{Files, Paths}

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.mllib.recommendation.{Rating, MatrixFactorizationModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

import scala.collection.Map


object ALSMusicToGo{

  var MAX_PETICIONES = 5
  val LOCAL_PATH="/home/david/Pragsis/Practica2/"
  var INPUT_DATA = LOCAL_PATH+"ratings/test"
  var MODEL_PATH = LOCAL_PATH+"model"
  var STREAMING_DATA = LOCAL_PATH+"streaming/"

  val TIME =1

  /**
    * Main
    *
    * @param args
    */
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("ALSMusicToGo")
    if(args.length == 0){
      println("Using default values")
      conf.setMaster("local[4]")
    }else{
      INPUT_DATA = args(0)
      MODEL_PATH = args(1)
      STREAMING_DATA = args(2)
      MAX_PETICIONES = Integer.valueOf(args(3))
    }


    val sc = new SparkContext(conf)

    LogManager.getRootLogger.setLevel(Level.ERROR)


    // Declaracion acumulador
    val accum = sc.accumulator(0,"ContadorPeticiones")

    val datosReproducciones = ProcessFile.calculateRating(sc,INPUT_DATA)

    // mapa idgrupo:nombregrupo
    var datosGrupos = datosReproducciones.map(dato=>(dato.idGrupo,dato.nombreGrupo)).reduceByKey((a:String,b:String)=>b).collectAsMap()

    // primera carga del modelo
    var storedModel = getModel(sc,false,false)



    var ssc = defineStreaming(sc,accum,storedModel,datosReproducciones,datosGrupos)
    println("Nuevo contexto stream cargado")
    println("----------------------------------")
    ssc.start()

    while(true){
      Thread.sleep(500)

      if(accum.value >= MAX_PETICIONES){

        accum.setValue(0)
        ssc.stop(false,true)

        // generar el nuevo modelo
        storedModel = getModel(sc,true,true)

        println("Generando nuevo diccionario de grupos")
        val reps = ProcessFile.calculateRating(sc,INPUT_DATA)
        datosGrupos = reps.map(dato=>(dato.idGrupo,dato.nombreGrupo)).reduceByKey((a:String,b:String)=>b).collectAsMap()

        println("Borrando datos streaming anterior")
        var file  = new File(STREAMING_DATA)
        file.delete()

        ssc = defineStreaming(sc,accum,storedModel,reps,datosGrupos)
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
  def defineStreaming(sc: SparkContext, accum:Accumulator[Int], modelo:MatrixFactorizationModel,ratings: RDD[CompareModels.DatosUsuario],datosGrupos: Map[Int, String]): StreamingContext ={
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
              recommendArtists(modelo,campos(0),ratings,datosGrupos)
            }catch {
              case e: Exception => {
                println("No hay recomendaciones para usuario "+campos(0)+":")
                println("----------------------------------")
              }
            }
          }else{
            println("No hay recomendaciones actualizadas para el usuario "+campos(0))
            println("----------------------------------")
          }
        })

        // Escritura del fichero de streaming
        val cosa = rdd.map(linea=>{
          var campos = linea.split("::")
          campos(0)+":##:"+campos(0).hashCode+":##:"+campos(3)+":##:"+campos(3).hashCode+":##:"+1
        }).saveAsTextFile(STREAMING_DATA)
      }
    })
    ssc
  }


  def updateNombres(values:Seq[String],state: Option[String])={

  }


  /**
    * Recomienda grupos para un determinado usuario
    *
    * @param modelo
    * @param userid
    * @param datosGrupos
    */
  def recommendArtists(modelo:MatrixFactorizationModel,userid: String, ratings: RDD[CompareModels.DatosUsuario],datosGrupos: Map[Int,String]){
    // Artistas recomendados
    val artistsRecommended = modelo.recommendProducts(userid.hashCode,10)

    // Grupos escuchados por el usuario
    val userGrupos = ratings.filter(datos=> datos.idUsuario==userid.hashCode).map(x=>x.idGrupo).collect()

    // Eliminar grupos ya escuchados
    val validArtists = artistsRecommended.filter(rating=> !userGrupos.contains(rating.product))

    // Ordenar y obtenernombre del grupo
    val listArtistsRecom = validArtists.sortBy(- _.rating).map(r=>(datosGrupos(r.product),r.rating))

    println("Recomendaciones para usuario "+userid+":")
    listArtistsRecom.foreach(println)
    println("----------------------------------")

    // Escritura a Hbase
    //listArtistsRecom.foreach({case (artist,rate)=> HBaseManager.saveToHBase(userid,artist,rate)} )

  }


  /**
    * Generacion modelo de prediccion
    *
    * @param sc
    * @param overrideModel
    * @param isStreaming
    * @return
    */
  def getModel(sc: SparkContext, overrideModel: Boolean, isStreaming: Boolean):MatrixFactorizationModel={
    if (Files.exists(Paths.get(MODEL_PATH)) && !overrideModel){
      println("Cargando modelo de disco...")
      MatrixFactorizationModel.load(sc, MODEL_PATH)
    } else {
      println("Generando modelo")
      val test_dataset = ProcessFile.getCompleteData(sc,isStreaming,INPUT_DATA,STREAMING_DATA)
      val test_rating = ProcessFile.calculateRating(sc,INPUT_DATA)
      CompareModels.calculateModel(sc,MODEL_PATH,test_rating)
    }
  }

}
