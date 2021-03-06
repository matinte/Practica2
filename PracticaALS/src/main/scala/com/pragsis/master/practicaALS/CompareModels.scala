package com.pragsis.master.practicaALS

import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

object CompareModels {

	case class DatosUserPlays(nombreUsuario:String,idUsuario:Int,nombreGrupo:String,idGrupo:Int,plays:Integer)
	case class DatosUsuario(nombreUsuario:String,idUsuario:Int,nombreGrupo:String,idGrupo:Int,rating:Double)

	def loadDatasetRatings(sc: SparkContext, path: String):RDD[Rating]={

		val ratings = sc.textFile(path).map(line=> {
			val array = line.split(":##:")
			try {
				Rating(array(1).toInt, array(3).toInt, array(4).toDouble)
			} catch{
				case e: Exception => {
					println(e.getMessage)
					println(line)
					Rating(0,0,0.0)
				}
			}
		})
		ratings
	}

	def loadDatasetDatos(sc: SparkContext, path: String):RDD[DatosUsuario]={

		val ratings = sc.textFile(path).map(line=> {
			val array = line.split(":##:")
			try {
				DatosUsuario(array(0),array(1).toInt,array(2),array(3).toInt,array(4).toDouble)
			} catch{
				case e: Exception => {
					println(e.getMessage)
					println(line)
					DatosUsuario("",0,"",0,0.0)
				}
			}
		})
		ratings
	}

	/**
		* Calcula un modelo de prediccion y lo salva en disco, borrando el modelo anterior
		* Usamos mejores rank, numero iteraciones y lambda btenidos en método compareModels(): 
		* rank = 10
		* num_iteraciones = 10
		* lambda = 10
		* @param sc
		* @param modelPath
		* @param dataset
    * @return
    */
	def calculateModel(sc: SparkContext, modelPath: String, dataset: RDD[DatosUsuario]):MatrixFactorizationModel={

		val datasetRating = dataset.map { case DatosUsuario(user, userid, artist, artistid, rate) => Rating(userid, artistid, rate) }

		println("Entrenando modelo")
		// usamos los parametros con mejores valores obtenidos en compareModels
		val model = ALS.train(datasetRating, 10, 10, 0.01) 
		println("Borrando modelo anterior")
		val path = new Path(modelPath)
		val conf = new Configuration()
		val fs = FileSystem.get(conf)
		fs.delete(path,true)
		println("Salvando modelo a disco")
		model.save(sc, modelPath)
		model
	}

	/**
		* Compara tres modelos de prediccion
		* @param sc
		* @param pathTrain
		* @param pathValid
		* @param pathTest
    */
	def compareModel(sc: SparkContext, pathTrain: String, pathValid: String, pathTest: String):Unit={

	  // calculate rating for each part and match to Rating data
	  val training_rating = ProcessFile.calculateRating(sc,pathTrain).map { case DatosUsuario(user, userid, artist, artistid, rate) => Rating(userid, artistid, rate) }
		val validation_rating = ProcessFile.calculateRating(sc,pathValid).map { case DatosUsuario(user, userid, artist, artistid, rate) => Rating(userid, artistid, rate) }
		val test_rating = ProcessFile.calculateRating(sc,pathTest).map { case DatosUsuario(user, userid, artist, artistid, rate) => Rating(userid, artistid, rate) }
		
		// cache datasets
		val datasetTrain = training_rating.cache()
	  val datasetValidation = validation_rating.cache()
	  val datasetTest = test_rating.cache()

		// train models with training dataset and different configuration parameters: lambda, rank, num_iterations
		val model1 = ALS.train(datasetTrain, 10, 10, 0.01) 
		val model2 = ALS.train(datasetTrain, 10, 10, 0.1) 
		val model3 = ALS.train(datasetTrain, 5, 10, 0.1) 

		// test with validation dataset
		val usersArtists = datasetValidation.map { case Rating(user, artist, rate) =>  (user, artist) }
		//
		val ratesAndPreds = datasetValidation.map { case Rating(user, artist, rate) => ((user, artist), rate)}

		// evaluate each model on validation dataset
		val predictions1 = model1.predict(usersArtists).map { case Rating(user, artist, rate) =>  ((user, artist), rate) }
		val predictions2 = model2.predict(usersArtists).map { case Rating(user, artist, rate) =>  ((user, artist), rate) }
		val predictions3 = model3.predict(usersArtists).map { case Rating(user, artist, rate) =>  ((user, artist), rate) }


		// join rates and predictions
		val joinRatesAndPred1 = ratesAndPreds.join(predictions1)
		val MSE1 = joinRatesAndPred1.map { case ((user, artist), (r1, r2)) =>
			val err = (r1 - r2)
			err * err
		}.mean()

		val joinRatesAndPred2 = ratesAndPreds.join(predictions2)
		val MSE2 = joinRatesAndPred2.map { case ((user, artist), (r1, r2)) =>
			val err = (r1 - r2)
			err * err
		}.mean()

		val joinRatesAndPred3 = ratesAndPreds.join(predictions3)
		val MSE3 = joinRatesAndPred3.map { case ((user, artist), (r1, r2)) =>
			val err = (r1 - r2)
			err * err
		}.mean()

		println("Mean Squared Error predictions1= " + MSE1)
		println("Mean Squared Error predictions2= " + MSE2)
		println("Mean Squared Error predictions3= " + MSE3)

		// evaluate with test dataset
		val finalModel = model1 //********model1, model2, model3, ....
		val usersArtistsTest = datasetTest.map { case Rating(user, artist, rate) =>  (user, artist) }
		val ratesAndPredsTest = datasetTest.map { case Rating(user, artist, rate) => ((user, artist), rate)}
		val finalPred = finalModel.predict(usersArtistsTest).map { case Rating(user, artist, rate) =>  ((user, artist), rate) }
		val joinRatesAndFinalPred = ratesAndPredsTest.join(finalPred)
		val MSE_final = joinRatesAndFinalPred.map { case ((user, artist), (r1, r2)) =>
			val err = (r1 - r2)
			err * err
		}.mean()
		println("Mean Squared Error Final Model Prediction= " + MSE_final)

	}
}