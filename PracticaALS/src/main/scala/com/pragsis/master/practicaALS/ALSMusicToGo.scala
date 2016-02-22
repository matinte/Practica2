package com.pragsis.master.practicaALS

import java.nio.file.{Files, Paths}
import com.pragsis.master.practicaALS.CompareModels.DatosUsuario
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS
import scala.collection.Map

object ALSMusicToGo {

  val LOCAL_PATH="/home/cloudera/Desktop/Practica2/"
  val NUMBER_CORES = 2
	var INPUT_TEST = LOCAL_PATH+"ratings/test"
	var INPUT_TRAINING = LOCAL_PATH+"ratings/training"
	var INPUT_STREAMING = LOCAL_PATH+"ratings/streaming"
	var MODEL_PATH = LOCAL_PATH+"model"


	def main(args: Array[String]) {
		// configure Spark Context
		val conf = new SparkConf().setAppName("CompareModels")

		// Se comprueba ejecucion en local
		if(args.length == 0 ){
			conf.setMaster("local[" + NUMBER_CORES + "]")
		}else{
			INPUT_TEST = args(0)
			MODEL_PATH = args(1)
		}

		val sc = new SparkContext(conf)
		LogManager.getRootLogger.setLevel(Level.ERROR)

		val data = CompareModels.loadDatasetDatos(sc,INPUT_TEST)
		println("Total number of ratings: " + data.count())
		println("Total number of groups rated: " + data.map(_.idGrupo).distinct().count())
		println("Total number of users who rated artists: " + data.map(_.idUsuario).distinct().count())

		// mapa idgrupo:nombregrupo
	  val datosGrupos = data.map(dato=>(dato.idGrupo,dato.nombreGrupo)).reduceByKey((a:String,b:String)=>b).collectAsMap()
		recommendArtists(sc,"e8c78120e88376654afac70051d09919676c1999",data,datosGrupos)

	}


	def getModel(sc: SparkContext, overrideModel: Boolean, isStreaming: Boolean):MatrixFactorizationModel={
		if (Files.exists(Paths.get(MODEL_PATH)) && !overrideModel){
			println("Cargando modelo de disco...")
			MatrixFactorizationModel.load(sc, MODEL_PATH)
		} else {
			println("Generando modelo")
			val test_dataset = ProcessFile.getCompleteData(sc,isStreaming)
			val test_rating = ProcessFile.calculateRating(sc)
		  CompareModels.calculateModel(sc,MODEL_PATH,test_rating)
		}
	}

	def recommendArtists(sc: SparkContext,userid: String,data: RDD[CompareModels.DatosUsuario], datosGrupos: Map[Int,String]){

		println("Buscando recomendaciones para usuario: "+userid)
		//1- calcular modelo
		val storedModel = getModel(sc,false, false)

		// Grupos que escucha el usuario
		val userGrupos = data.filter(datos=>datos.idUsuario == userid.hashCode).map(x=>x.idGrupo).collect()
		println("Total number of groups rated by this user: " + userGrupos.distinct.length )
		
		// Artistas recomendados
		val artistsRecommended = storedModel.recommendProducts(userid.hashCode,300)

		val listArtistsRecom = artistsRecommended.filter(r=> !userGrupos.contains(r.product)).sortBy(- _.rating)
		  .map(r=>(datosGrupos(r.product),r.rating)).take(10)
		
		println("List recommended: ") 
		listArtistsRecom.foreach(println) 
		listArtistsRecom.foreach({case (artist,rate)=> HBaseManager.saveToHBase(userid,artist,rate)} )
		  
	}

}      



