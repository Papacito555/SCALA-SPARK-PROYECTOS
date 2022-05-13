package com.TarantulaStudios.spark.Capacitacion.CapacitacionRDD

import org.apache.spark.sql.SparkSession

object RDDRepartitionExample extends App {

  val spark:SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("SReparticiones")
    .getOrCreate()

  val rdd = spark.sparkContext.parallelize(Range(0,20))
  println("From local[*]"+rdd.partitions.size)

  val rdd1 = spark.sparkContext.parallelize(Range(0,20), 6)
  println("parallelize : "+rdd1.partitions.size)

  rdd1.partitions.foreach(f=> f.toString)
  val rddFromFile = spark.sparkContext.textFile("src/main/resources/test.txt",9)

  println("Archivo de Texto : "+rddFromFile.partitions.size)

  rdd1.saveAsTextFile("c:/tmp/partition")
  val rdd2 = rdd1.repartition(4)
  println("Tamaño Reparticion : "+rdd2.partitions.size)

  rdd2.saveAsTextFile("c:/tmp/re-partition")

  val rdd3 = rdd1.coalesce(4)
  println("Tamaño Reparticion : "+rdd3.partitions.size)

  rdd3.saveAsTextFile("c:/tmp/coalesce")
}
