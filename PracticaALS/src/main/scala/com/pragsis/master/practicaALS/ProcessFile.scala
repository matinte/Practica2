package com.pragsis.master.practicaALS

import java.io.File
import java.lang.Boolean

import com.pragsis.master.practicaALS.CompareModels.{DatosUserPlays, DatosUsuario}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ProcessFile {

  val MAX_EXPLICIT_RATING = 10.0 // --> se utiliza el mismo valor que el mejor rank en compareModel()
  val LOCAL_PATH="/home/david/Pragsis/Practica2/"
  var INPUT_FILE=LOCAL_PATH+"lastfm-dataset-360K/usersha1-artmbid-artname-plays.tsv"
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
    val completeData = getCompleteData(sc,false,INPUT_FILE,INPUT_STREAMING)

    // split del fichero total con user,user.id,group,group.id,plays
    val partes = completeData.randomSplit(Array(0.6,0.2,0.2),0L)

    // Escritura a fichero
    partes(0).saveAsTextFile(OUTPUT_FILE_TRAINING)
    partes(1).saveAsTextFile(OUTPUT_FILE_VALIDATION)
    partes(2).saveAsTextFile(OUTPUT_FILE_TEST)
  }

  /**
    * Escribe el nuevo fichero de datos que alimenta el modelo a partir de un fichero de entrada
    * Opcionalmente, concatena con el fichero que se ha ido escribiendo en la fase de streaming
    * @param sc
    * @param concatStreaming
    * @param pathOldData
    * @param pathStreaming
    * @return
    */
  def getCompleteData(sc: SparkContext, concatStreaming: Boolean, pathOldData:String, pathStreaming:String):RDD[String]= {

    // lectura de fichero
    val file = sc.textFile(pathOldData)


    println("Parseando datos de entrenamiento")
    val data = file.map(linea => {
      val campos = linea.split('\t')
      val plays = Integer.parseInt(campos(3).trim.replaceFirst("^0+(?!$)", ""))
      val user = campos(0).trim
      val artist = campos(2).trim
      ((user, user.hashCode(), artist, artist.hashCode()), plays)
    })

    if (concatStreaming) {
      // lectura de fichero
      println("Parseando datos de streaming")
      val strfile = sc.textFile(pathStreaming)
      val dataStr = strfile.map(linea => {
        val campos = linea.split(":##:")
        val plays = 1
        val user = campos(0).trim
        val artist = campos(2).trim
        ((user, user.hashCode(), artist, artist.hashCode()), plays)
      }).reduceByKey((k, v) => k + v)

      // join (suma de los datos de ambos rdds)
      val tuplas = data.union(dataStr).reduceByKey(_+_)

      // return all data
      val dataFinal = tuplas.map(tupla => DatosUserPlays(tupla._1._1, tupla._1._2, tupla._1._3, tupla._1._4, tupla._2))
      val dataString = dataFinal.map(datos =>datos.nombreUsuario + "\t" + datos.idGrupo + "\t" + datos.nombreGrupo + "\t" + datos.plays)

      // Borrar directorio datos y volverlo a escribir

      // generacion nuevos datos
      dataString.saveAsTextFile("/tmp/datamodel")

      // Borrado datos antiguos
      val path = new Path(pathOldData)
      val conf = new Configuration()
      val fs = FileSystem.get(conf)
      fs.delete(path,true)

      // Renombrado a fichero de datos
      val forigen = new File("/tmp/datamodel")
      val fdestino = new File(pathOldData)
      forigen.renameTo(fdestino)

      // Borrado datos intermedios
      forigen.delete()

      dataString
    } else {
      // return all data
      val dataFinal = data.map(tupla => DatosUserPlays(tupla._1._1, tupla._1._2, tupla._1._3, tupla._1._4, tupla._2))
      val dataString = dataFinal.map(datos =>datos.nombreUsuario + "\t" + datos.idGrupo + "\t" + datos.nombreGrupo + "\t" + datos.plays)

      dataString
    }

  }

  /**
    * Lee el fichero de entrada con datos de reproducciones y calcula el rdd de ratings
    * que necesita el modelo de prediccion
    * @param sc
    * @param filePath
    * @return
    */
  def calculateRating(sc: SparkContext,filePath:String):RDD[CompareModels.DatosUsuario]={

    // lectura de fichero
    val file = sc.textFile(filePath)
    // Obtencion de rdd con los usurios, grupos y numeros de reproducciones
    val usuarios = file.map(linea=>{
      val campos = linea.split("\t")
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
    ratings
  }

}