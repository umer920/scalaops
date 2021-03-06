package com.platalytics

import org.apache.spark._
import org.apache.spark.SparkConf
//import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.SparkContext._

class HRGenerator {
  
  def HighestRevenueGenerating(args : Array[String]) = {
    val conf = new SparkConf().setAppName("Data Processor");
     //conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer");
     //conf.set("spark.kryo.registrator", "com.platalytics.spark.MyRegistrator"); 
     val sc = new SparkContext(conf);  

     var price_table = sc.textFile("Items.csv").map(line => {
      val parts = line.split(",")
      (parts(0).toInt, parts(3).toDouble)
     })
     price_table = sc.parallelize(price_table.take(10))
     
     var qty_table = sc.textFile("Purchases.csv").map(line => {
      val parts = line.split(",")
      (parts(3).toInt, parts(4).toDouble)
     })
     qty_table = sc.parallelize(qty_table.take(10))
     
     qty_table  = qty_table.reduceByKey((a, b) => a + b)
     
     var highest_revenue = price_table.join(qty_table)
     
     var revenue = highest_revenue.map(f => (f._1 , f._2._1 * f._2._2))
     
     var result = revenue.collect.toSeq.sortBy(_._2)
     result = result.reverse
     result.foreach(f=>println(f._1 + " , "+f._2))
  }
  

}