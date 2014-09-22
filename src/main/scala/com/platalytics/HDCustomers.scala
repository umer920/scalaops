package com.platalytics

import org.apache.spark._
import org.apache.spark.SparkConf
//import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.SparkContext._

class HDCustomers {
  
  def HighestDissatisfiedCustomer(args : Array[String]) = {
    val conf = new SparkConf().setAppName("Data Processor");
     //conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer");
     //conf.set("spark.kryo.registrator", "com.platalytics.spark.MyRegistrator"); 
     val sc = new SparkContext(conf);  
     
     var purchases = sc.textFile("Purchases.csv").map(line => {
      val parts = line.split(",")
      (parts(1).toInt, parts(4).toDouble)
     })
     
     var returns= sc.textFile("Returns.csv").map(line => {
      val parts = line.split(",")
      (parts(1).toInt, parts(4).toDouble)
     })
     
     
     
     
     //var user_expenditure = user_purchases.map(f => (f._1 , f._2._1 * f._2._2))
     var user_purchases = purchases.reduceByKey((a, b) => a + b)
     var user_returns = returns.reduceByKey((a, b) => a + b)
     
     var result = user_purchases.join(user_returns)
     
     //result.map
     
     var ratio = result.map(f => (f._1 , f._2._1 / f._2._2))
     var fresult = ratio.collect.toSeq.sortBy(_._2)
     fresult.foreach(f=>println(f._1 + " , "+f._2))
     
     
     
  }

}