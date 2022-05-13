package com.TarantulaStudios.spark.Funciones

import org.apache.spark.sql.SparkSession

object Ejemplo_Funcion extends App {

  val spark = SparkSession.builder()
    .appName("Ejemplos Funciones")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  //Ejemplo Agregación

  val ListaRDD = spark.sparkContext.parallelize(List(1, 2, 3, 4, 5, 3, 2))

  def parm0 = (acu: Int, v: Int) => acu + v

  def parm1 = (acu1: Int, acu2: Int) => acu1 + acu2

  println("Salida 1: " + ListaRDD.aggregate(0)(parm0, parm1))

  val inputRDD = spark.sparkContext.parallelize(List(("Z", 1), ("A", 20), ("B", 40), ("C", 30), ("B", 60), ("B", 30)))

  def parm3 = (acu: Int, v: (String, Int)) => acu + v._2

  def parm4 = (acu1: Int, acu2: Int) => acu1 + acu2

  println("Salida 2: " + inputRDD.aggregate(0)(parm3, parm4))

  println("Numero de particiones: " + ListaRDD.getNumPartitions)

  println("Salida guanga: " + ListaRDD.aggregate(1)(parm0, parm1))
}
