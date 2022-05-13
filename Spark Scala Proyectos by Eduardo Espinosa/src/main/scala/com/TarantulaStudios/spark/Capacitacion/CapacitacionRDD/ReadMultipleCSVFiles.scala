package com.TarantulaStudios.spark.Capacitacion.CapacitacionRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ReadMultipleCSVFiles extends App {

  val spark:SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("Leer Archivos CSV")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  println("spark lee archivos csv a  RDD")
  val rddFromFile = spark.sparkContext.textFile("C:/tmp/files/text01.csv")
  println(rddFromFile.getClass)

  val rdd = rddFromFile.map(f=>{
    f.split(",")
  })

  println("Interactuando RDD")
  rdd.foreach(f=>{
    println("Col1:"+f(0)+",Col2:"+f(1))
  })
  println(rdd)

  println("Get data Using collect")
  rdd.collect().foreach(f=>{
    println("Col1:"+f(0)+",Col2:"+f(1))
  })

  println("rLee todo los archivos CSV a  RDD")
  val rdd2 = spark.sparkContext.textFile("C:/tmp/files/*")
  rdd2.foreach(f=>{
    println(f)
  })

  println("El Bromas")
  val rdd3 = spark.sparkContext.textFile("C:/tmp/files/text*.csv")
  rdd3.foreach(f=>{
    println(f)
  })

  println("lee multiples archivos")
  val rdd4 = spark.sparkContext.textFile("C:/tmp/files/text01.csv,C:/tmp/files/text02.csv")
  rdd4.foreach(f=>{
    println(f)
  })

}

