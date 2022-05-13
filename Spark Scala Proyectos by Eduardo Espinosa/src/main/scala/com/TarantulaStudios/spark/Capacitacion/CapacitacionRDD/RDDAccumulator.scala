package com.TarantulaStudios.spark.Capacitacion.CapacitacionRDD

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object RDDAccumulator extends App {

  val spark = SparkSession.builder()
    .appName("Acumulador")
    .master("local")
    .getOrCreate()

  val longAcc = spark.sparkContext.longAccumulator("SumAccumulator")

  val rdd = spark.sparkContext.parallelize(Array(1, 2, 3))

  rdd.foreach(x => longAcc.add(x))
  println(longAcc.value)
}
