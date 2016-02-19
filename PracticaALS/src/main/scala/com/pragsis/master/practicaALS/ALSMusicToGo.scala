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

	  /* user b2d47bbf36f17961ccea17d40e98c7b3c6c1fcc2 --> generate model. Output:
      Total number of ratings: 3510497
      Total number of groups rated: 143284
      Total number of users who rated artists: 358995
      Buscando recomendaciones para usuario: b2d47bbf36f17961ccea17d40e98c7b3c6c1fcc2
      Generando modelo
      Total number of groups rated by this user: 9
      List recommended:
      (ガレージシャンソンショー,16.93687287453816)
      (chance,16.689002067277446)
      (hermínia silva,16.227460269540554)
      (the crabb family,15.936585882105067)
      (orchestra super mazembe,15.813589421443455)
      (deliverance,15.242531067884068)
      (guy penrod,13.552663154549679)
      (bebeto,13.209813366338823)
      (martin iveson,13.063593024136877)
      (flesh & space,13.039111548329625)
	  */
	  
	  /* User e8c78120e88376654afac70051d09919676c1999 --> load model. Output:
	    Total number of ratings: 3510497
      Total number of groups rated: 143284
      Total number of users who rated artists: 358995
      Buscando recomendaciones para usuario: e8c78120e88376654afac70051d09919676c1999
      Cargando modelo de disco... 
	    Total number of groups rated by this user: 10
      List recommended: 
      (lungbutter,6.862049034132991)
      (martin iveson,6.6723646275165605)
      (j. axel,6.639055381609971)
      (Олег Медведев,6.252525982492234)
      (canton jones,6.242121801068594)
      (krynitza,6.164115231209973)
      (lost dogs,6.054791681579534)
      (gatto marte,5.877985884143095)
      (adrian legg,5.8630453662102235)
      (laura sullivan,5.855440997396269)
	   */
		recommendArtists(sc,"e8c78120e88376654afac70051d09919676c1999",data,datosGrupos)

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

	def recommendArtists(sc: SparkContext,userid: String,data: RDD[CompareModels.DatosUsuario], datosGrupos: Map[Int,String]){

		println("Buscando recomendaciones para usuario: "+userid)
		//1- calcular modelo
		val storedModel = getModel(sc,false)

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



