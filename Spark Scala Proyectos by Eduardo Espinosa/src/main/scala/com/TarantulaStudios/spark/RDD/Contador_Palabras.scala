package com.TarantulaStudios.spark.RDD

import org.apache.spark.sql.SparkSession

object Contador_Palabras {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Contador Palabras")
      .getOrCreate()

    val textFile = spark.sparkContext.textFile("C:/Users/SDS-Usuario/Downloads/spark/data/data/Sacrifice.txt")

    //Contar Palabras del texto
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    counts.collect.foreach(println _)

    val repeticion = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .map(item => item.swap)
      .sortByKey(false)
      .take(10)
    repeticion.foreach(println _)


    textFile.foreach(println)
  }
}
