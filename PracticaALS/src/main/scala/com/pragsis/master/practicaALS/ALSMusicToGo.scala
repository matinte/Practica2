package com.pragsis.master.practicaALS

import java.nio.file.{Files, Paths}

import com.pragsis.master.practicaALS.CompareModels.DatosUsuario
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.rdd.RDD

object ALSMusicToGo {


	var INPUT_TEST = "/home/david/Pragsis/Practica2/ratings/test"
	var INPUT_TRAINING = "/home/david/Pragsis/Practica2/ratings/training"
	var MODEL_PATH = "/home/david/Pragsis/Practica2/model"


	def main(args: Array[String]) {
		// configure Spark COntext
		val conf = new SparkConf().setAppName("CompareModels")

		// Se comprueba ejecucion en local
		if(args.length == 0 ){
			conf.setMaster("local[8]")
		}else{
			INPUT_TEST = args(0)
			MODEL_PATH = args(1)
		}

		val sc = new SparkContext(conf)
		LogManager.getRootLogger.setLevel(Level.ERROR)

		val data = CompareModels.loadDatasetDatos(sc,INPUT_TEST)

		// mapa idgrupo:nombregrupo
		val datosGrupos = data.map(dato=>(dato.idGrupo,dato.nombreGrupo)).reduceByKey((a:String,b:String)=>b).collectAsMap()

		recommendArtists(sc,"fadcddbe4e87f0c3c1af6913f8b619ad3be376c0",data,datosGrupos)

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

	def recommendArtists(sc: SparkContext,userid: String,data: RDD[DatosUsuario], datosGrupos: scala.collection.Map[Int,String]){

		println("Buscando recomendaciones para usuario: "+userid)
		//1- calcular modelo
		val storedModel = getModel(sc,false)

		// Grupos que escucha el usuario
		val userGrupos = data.filter(datos=>datos.idUsuario == userid.hashCode).map(x=>x.idGrupo).collect()

		// Artistas recomendados
		val artistsRecommended = storedModel.recommendProducts(userid.hashCode,300)

		artistsRecommended.filter(r=> !userGrupos.contains(r.product)).sortBy(- _.rating)
		  .map(r=>(datosGrupos(r.product),r.rating)).take(10).foreach(println)

	}

}      



