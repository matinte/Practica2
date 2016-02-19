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
import org.apache.spark.rdd.RDD

object CompareModels {

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

	def calculateModel(sc: SparkContext, modelPath: String, pathTrain: String):MatrixFactorizationModel={
			
			val training = loadDatasetRatings(sc,pathTrain)
			
			// train models with training dataset and different configuration parameters: lambda, rank, num_iterations
			val model = ALS.train(training, 10, 10, 0.01) //(new ALS().setRank(10).setIterations(10).setLambda(0.01).(training))
			val modelRun = (new ALS().setRank(10).setIterations(10).run(training))
			modelRun.save(sc, modelPath)
			modelRun
			}
	
	def compareModel(sc: SparkContext, pathTrain: String, pathValid: String, pathTest: String):Unit={
			
	    
			val training = loadDatasetRatings(sc,pathTrain).cache()
			val validation = loadDatasetRatings(sc,pathValid).cache()
			val test = loadDatasetRatings(sc,pathTest).cache()

			// train models with training dataset and different configuration parameters: lambda, rank, num_iterations
			val model1 = ALS.train(training, 10, 10, 0.01) //(new ALS().setRank(10).setIterations(10).setLambda(0.01).(training))
			val model2 = ALS.train(training, 20, 10, 0.01) //(new ALS().setRank(20).setIterations(10).setLambda(0.01).run(training))
			val model3 = ALS.train(training, 30, 10, 0.01) //(new ALS().setRank(30).setIterations(10).setLambda(0.01).run(training))

			// test with validation dataset
			val usersArtists = validation.map { case Rating(user, artist, rate) =>  (user, artist) }
			//
			val ratesAndPreds = validation.map { case Rating(user, artist, rate) => ((user, artist), rate)}

			// evaluate each model on validation dataset
			val predictions1 = model1.predict(usersArtists).map { case Rating(user, artist, rate) =>  ((user, artist), rate) }
			val predictions2 = model2.predict(usersArtists).map { case Rating(user, artist, rate) =>  ((user, artist), rate) }
			val predictions3 = model3.predict(usersArtists).map { case Rating(user, artist, rate) =>  ((user, artist), rate) }


			//
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

			//			println("Mean Squared Error predictions1= " + MSE1)
			//			println("Mean Squared Error predictions2= " + MSE2)
			//			println("Mean Squared Error predictions3= " + MSE3)

			// evaluate with test dataset
			val finalModel = model1 //********model1, model2, model3, ....
			val usersArtistsTest = test.map { case Rating(user, artist, rate) =>  (user, artist) }
			val ratesAndPredsTest = test.map { case Rating(user, artist, rate) => ((user, artist), rate)}
			val finalPred = finalModel.predict(usersArtistsTest).map { case Rating(user, artist, rate) =>  ((user, artist), rate) }
			val joinRatesAndFinalPred = ratesAndPredsTest.join(finalPred)
					val MSE_final = joinRatesAndFinalPred.map { case ((user, artist), (r1, r2)) =>
					val err = (r1 - r2)
					err * err
			}.mean()
			println("Mean Squared Error Final Model Prediction= " + MSE_final)
			
			}
	}