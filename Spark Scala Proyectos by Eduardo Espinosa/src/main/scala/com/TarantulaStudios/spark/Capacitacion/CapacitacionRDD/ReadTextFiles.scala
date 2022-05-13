package com.TarantulaStudios.spark.Capacitacion.CapacitacionRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ReadTextFiles extends App {

  val spark:SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("Leer Archivos de Texto")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  println("##Spark anda leyendo un libro vaquero desde RDD")
  val rddFromFile = spark.sparkContext.textFile("src/main/resources/csv/text01.txt")
  println(rddFromFile.getClass)

  println("##Obteniendo datos usando Colect")
  rddFromFile.collect().foreach(f=>{
    println(f)
  })

  println("##Leer multiples archivos desde RDD")
  val rdd4 = spark.sparkContext.textFile("src/main/resources/csv/text01.txt," +
    "src/main/resources/csv/text02.txt")
  rdd4.foreach(f=>{
    println(f)
  })

  println("##leer archivos de texto basados en caracteres comodín")
  val rdd3 = spark.sparkContext.textFile("src/main/resources/csv/text*.txt")
  rdd3.foreach(f=>{
    println(f)
  })

  println("##leer todos los archivos de texto de un directorio a un solo RDD")
  val rdd2 = spark.sparkContext.textFile("src/main/resources/csv/*")
  rdd2.foreach(f=>{
    println(f)
  })

  println("##leer archivos de texto completos")
  val rddWhole:RDD[(String,String)] = spark.sparkContext.wholeTextFiles("src/main/resources/csv/text01.txt")
  println(rddWhole.getClass)
  rddWhole.foreach(f=>{
    println(f._1+"=>"+f._2)
  })
}

