package com.TarantulaStudios.spark.Capacitacion.CapacitacionRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

object RDDFromDataUsingParallelize {

  def main(args: Array[String]): Unit = {
      val spark:SparkSession = SparkSession.builder()
        .master("local[3]")
        .appName("Particiones RDD")
        .getOrCreate()
      val rdd:RDD[Int] = spark.sparkContext.parallelize(List(1,2,3,4,5))
      val rddCollect:Array[Int] = rdd.collect()
      println("Numero de particiones: "+rdd.getNumPartitions)
      println("Action: primer elemento: "+rdd.first())
      println("Action: RDD covertido a un Arreglo[Int] : ")
      rddCollect.foreach(println)

  }
}
