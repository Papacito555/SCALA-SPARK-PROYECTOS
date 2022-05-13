package com.TarantulaStudios.spark.Funciones

import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object Flat_Map extends App {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Mapa Plano")
    .getOrCreate()

  val datos = Seq(
    "Libro Vaquero",
    "Heat 2",
    "Harry Potter",
    "Constitución Politica 1917",
    "Libro Vaquero",
    "Heat 2",
    "Harry Potter",
    "Heat 2",
  )

  val rdd = spark.sparkContext.parallelize(datos)
  rdd.foreach(println)

  val rdd1 = rdd.flatMap(f => f.split(" "))
  rdd1.foreach(println)

  val arrayStructureData = Seq(
    Row("James Mcaboy", List("Ruby", "Java", "Python"), "NV"),
    Row("Michael Townley", List("Scala", "Cassandra", "Hive"), "CA"),
    Row("Samuel Houser", List("Spark", "MongoDB", "C++"), "FL")
  )


  val arrayStructSchema = new StructType()
    .add("Nombre", StringType)
    .add("Lenguajes", ArrayType(StringType))
    .add("Localización", StringType)

  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(arrayStructureData), arrayStructSchema)

  import spark.implicits._

  val df2 = df.flatMap(f => f.getSeq[String](1).map((f.getString(0), _, f.getString(2))))
    .toDF("Nombre", "Lenguajes", "Localizacion")
  df2.show(false)


}
