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


object ALSMusicToGo {
  
  def main(args: Array[String]) {
      if (args.length < 1) {
         System.err.println("Usage: ALS MusicToGo <tsvfile>")
         System.exit(1)
      }
      
      // configure Spark COntext
      val conf = new SparkConf().setMaster("local[*]").setAppName("ALS MusicToGo")
      val sc = new SparkContext(conf)
      
      // Configure the Streaming Context with a 1 second batch duration
      val ssc = new StreamingContext(conf,Seconds(1)) // 1 second ??
      
      // Load ratings RDD
      // val file = sc.textFile("/home/cloudera/Desktop/Practica1/dataset_practico1/lastfm-dataset-360K/usersha1-artmbid-artname-plays.tsv")
      val ratings = sc.textFile("/home/cloudera/Desktop/Practica2/ratings")
                      .map(_.split(';') match { case Array(user, artist, rate) => Rating(user.toInt, artist.toInt, rate.toDouble) })
                      
      // split the data in train, validation and test
      val parts = ratings.randomSplit(Array(0.6,0.2,0.2),0L)
      val training = parts(0).cache()
      val validation = parts(0).cache()
      val test = parts(0).cache()  
        
      // train models with training dataset and different configuration parameters: lambda, rank, num_iterations
      val model1 = ALS.train(training, 10, 10, 0.01) //(new ALS().setRank(10).setIterations(10).setLambda(0.01).(training)) 
      val model2 = ALS.train(training, 20, 10, 0.01) //(new ALS().setRank(20).setIterations(10).setLambda(0.01).run(training))
      val model3 = ALS.train(training, 30, 10, 0.01) //(new ALS().setRank(30).setIterations(10).setLambda(0.01).run(training))
      
      // evaluate each model on validation dataset
      val predictions1 = model1.predict(validation).map { case Rating(user, artist, rate) =>  ((user, artist), rate) }
      val predictions2 = model2.predict(validation).map { case Rating(user, artist, rate) =>  ((user, artist), rate) }
      val predictions3 = model3.predict(validation).map { case Rating(user, artist, rate) =>  ((user, artist), rate) }
      
     } 
}      
      
      

//      val   
//      
//      // Build the recommendation model using ALS
//      val rank = 10
//      val numIterations = 10
//      val model = ALS.train(ratings, rank, numIterations, 0.01)
//      val limit = 100
//      
//      // Evaluate the model on rating data
//      val usersProducts = ratings.map { case Rating(user, product, rate) =>
//        (user, product)
//      }
//      val predictions =
//        model.predict(usersProducts).map { case Rating(user, product, rate) =>
//          ((user, product), rate)
//        }
//      val ratesAndPreds = ratings.map { case Rating(user, product, rate) => ((user, product), rate)}
//      ratesAndPreds.join(predictions)
//      val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
//        val err = (r1 - r2)
//        err * err
//      }.mean()
//      println("Mean Squared Error = " + MSE)
//      
//      // set to 0 before loop
//      val numPlays = 0
//      
//      // loop during straming
//      while (true){
//          
//        numPlays=numStreaminngPlays
//          
//        if (numPlays > limit) {
//          //If the rating matrix is derived from another source of information (e.g., it is inferred from other signals), 
//          //you can use the trainImplicit method to get better results.
//          val alpha = 0.01
//          val lambda = 0.01
//          val model = ALS.trainImplicit(ratings, rank, numIterations, lambda, alpha)
//        }
//        
//        // Save and load model
//        model.save(sc, "target/tmp/myCollaborativeFilter")
//        val sameModel = MatrixFactorizationModel.load(sc, "target/tmp/myCollaborativeFilter")
//        
//        wait
//      }
//    

