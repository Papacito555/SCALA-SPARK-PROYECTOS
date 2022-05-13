package com.TarantulaStudios.spark.Capacitacion.CapacitacionRDD

import org.apache.spark.sql.SparkSession

object CreateEmptyRDD extends App{

  val spark:SparkSession = SparkSession.builder()
    .master("local[3]")
    .appName("Mi Primer RDD")
    .getOrCreate()

  val rdd = spark.sparkContext.emptyRDD
  val rddString = spark.sparkContext.emptyRDD[String]

  println(rdd)
  println(rddString)
  println("Numero de particiones: "+rdd.getNumPartitions)

  rddString.saveAsTextFile("C:/Users/SDS-Usuario/Downloads/spark/data/data/test5.txt")

  val rdd2 = spark.sparkContext.parallelize(Seq.empty[String])
  println(rdd2)
  println("Numero de particiones: "+rdd2.getNumPartitions)

  rdd2.saveAsTextFile("C:/Users/SDS-Usuario/Downloads/spark/data/data/test3.txt")

  // Par RDD

  type dataType = (String,Int)
  var pairRDD = spark.sparkContext.emptyRDD[dataType]
  println(pairRDD)

}
