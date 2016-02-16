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


class ALSMusicToGo {
  
  def main(args: Array[String]) {
      if (args.length < 1) {
         System.err.println("Usage: ALS MUsicToGo <tsvfile>")
         System.exit(1)
      }
      
      // configure Spark COntext
      val conf = new SparkConf().setAppName("ALS MusicToGo")
      val sc = new SparkContext(conf)
      
      // Configure the Streaming Context with a 1 second batch duration
      val ssc = new StreamingContext(conf,Seconds(1)) // 1 second ??
      
      // Load and parse the data
      val data = sc.textFile("/home/cloudera/Desktop/Practica2/usersha1-artmbid-artname-plays.tsv")
      val ratings = data.map(_.split('\t') match { 
        case Array(user, artist, rate) => (artist.toInt, rate.toDouble) //.map(words=>(words(0),words(1),words(2))
      })
      
      // converse ratings doubles into range 0..5 stars
      val ratingsMax = ratings.map{_.swap}.sortByKey(false).map(_.swap).takeOrdered(0)
      val ratingsMin = ratings.map{_.swap}.sortByKey(false).map(_.swap).takeOrdered(ratings.count().toInt)
      
      ratings.foreach(convertRate(r))
      
      // split the data in train, validation and test
      val   
      
      // Build the recommendation model using ALS
      val rank = 10
      val numIterations = 10
      val model = ALS.train(ratings, rank, numIterations, 0.01)
      val limit = 100
      
      // Evaluate the model on rating data
      val usersProducts = ratings.map { case Rating(user, product, rate) =>
        (user, product)
      }
      val predictions =
        model.predict(usersProducts).map { case Rating(user, product, rate) =>
          ((user, product), rate)
        }
      val ratesAndPreds = ratings.map { case Rating(user, product, rate) => ((user, product), rate)}
      ratesAndPreds.join(predictions)
      val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
        val err = (r1 - r2)
        err * err
      }.mean()
      println("Mean Squared Error = " + MSE)
      
      // set to 0 before loop
      val numPlays = 0
      
      // loop during straming
      while (true){
          
        numPlays=numStreaminngPlays
          
        if (numPlays > limit) {
          //If the rating matrix is derived from another source of information (e.g., it is inferred from other signals), 
          //you can use the trainImplicit method to get better results.
          val alpha = 0.01
          val lambda = 0.01
          val model = ALS.trainImplicit(ratings, rank, numIterations, lambda, alpha)
        }
        
        // Save and load model
        model.save(sc, "target/tmp/myCollaborativeFilter")
        val sameModel = MatrixFactorizationModel.load(sc, "target/tmp/myCollaborativeFilter")
        
        wait
      }
    

     

      

      

   }
  
}