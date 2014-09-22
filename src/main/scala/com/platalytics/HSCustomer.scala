package com.platalytics

import org.apache.spark._
import org.apache.spark.SparkConf
//import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.SparkContext._


class HSCustomer {
  
  def HighestSpendingCustomer(args : Array[String]) = {
    val conf = new SparkConf().setAppName("Data Processor");
     //conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer");
     //conf.set("spark.kryo.registrator", "com.platalytics.spark.MyRegistrator"); 
     val sc = new SparkContext(conf);  
     
     var user_purchases = sc.textFile("Purchases.csv").map(line => {
      val parts = line.split(",")
      (parts(1).toInt, (parts(4).toDouble, parts(5).toDouble))
     })
      user_purchases = sc.parallelize(user_purchases.take(10))
     
     var user_expenditure = user_purchases.map(f => (f._1 , f._2._1 * f._2._2))
     var result = user_expenditure.reduceByKey((a, b) => a + b)
     
     
     var res = result.collect.toSeq.sortBy(_._2)
     res = res.reverse
     println(res.head._1 + " , " + res.head._2)
     //res.foreach(f=>println(f._1 + " , "+f._2))
     
     
     
  }

}