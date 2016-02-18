package com.pragsis.master.practicaALS

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.Seconds
import java.nio.file.{Paths, Files}
import org.apache.spark.mllib.recommendation.Rating

object ALSMusicToGo {

	var INPUT_TEST = "/media/david/Elements/Practica2/ratings/test"
	var INPUT_TRAINING = "/media/david/Elements/Practica2/ratings/training"
  var MODEL_PATH = "/home/cloudera/practica2/als/finalmodel"
//  var STORED_MODEL = new MatrixFactorizationModel(0,null,null)

	
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
	}
	
	
	def getModel(sc: SparkContext, overrideModel: Boolean):MatrixFactorizationModel={
	  if (Files.exists(Paths.get(MODEL_PATH)) && !overrideModel){
				MatrixFactorizationModel.load(sc, MODEL_PATH) 
		} else {
		  CompareModels.calculateModel(sc,MODEL_PATH,INPUT_TRAINING)
		}
	}  
	
	def recommendArtists(sc: SparkContext,userid: String){

		//1- calcular modelo
	  val storedModel = getModel(sc,false)
		
		//2- load TEST_DATASET and filter by userid
	  val test = CompareModels.loadDataset(sc, INPUT_TEST).filter(x=>x.user.hashCode()!=userid.hashCode())
 
		//3- ratesAndPreds for all users in test
		val usersArtists = test.map { case Rating(user, artist, rate) =>  (user, artist) }
		
		//4- predict with model
	  val artistsRecommended = storedModel.predict(usersArtists).sortBy(r=>r.rating,false).take(10).foreach(println)
	  
	  //5- check if artist not in user playlist
		
	}
		
}      


