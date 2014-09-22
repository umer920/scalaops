package com.platalytics

import org.apache.spark._
import org.apache.spark.SparkConf
//import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.SparkContext._


class HRItems {
  
  def HihghestReturnedItems(args : Array[String]) {
	 
     val conf = new SparkConf().setAppName("Data Processor");
     //conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer");
     //conf.set("spark.kryo.registrator", "com.platalytics.spark.MyRegistrator"); 
     val sc = new SparkContext(conf);  

    var line = null
    var vectors = sc.textFile("Purchases.csv").map(line => {
      val parts = line.split(",")
      (parts(3).toInt, parts(4).toInt)
     })
      vectors = sc.parallelize(vectors.take(10))
     
     var reduced  = vectors.reduceByKey((a, b) => a + b)
     var result = reduced.collect.toSeq.sortBy(_._2)
     result = result.reverse
     result.foreach(f=>println(f._1 + " , "+f._2))
     
  }
}