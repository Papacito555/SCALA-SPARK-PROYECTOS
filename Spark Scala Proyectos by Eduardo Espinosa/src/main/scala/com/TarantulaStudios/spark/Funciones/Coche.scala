package com.TarantulaStudios.spark.Funciones

import org.apache.spark.sql.SparkSession

object  Coche{

  def main(args:Array[String]) ={
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("RDD Vehiculos")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val datos = spark.sparkContext.parallelize(List("Mclaren 720S", "Lamborghini Huracan", "Mclaren Ferrar Lamborghini", "Ford Chevrolet Dodge", "Mitsubishi Nissan Toyota", "Mclaren 720S", "Lamborghini Huracan", "Mclaren Ferrar Lamborghini", "Mclaren Chevrolet Dodge", "Mitsubishi Mclaren Toyota")
    )

    val wordsRDD = datos.flatMap(_.split(" "))
    val pairRDD = wordsRDD.map(f=>(f,1))
    pairRDD.foreach(println)

  println()
  println()
  println("Distinct ==>")
  println()
  pairRDD.distinct().foreach(println)

  println()
  println()
  println("Sort by Key ==>")
  println()
   val sort = pairRDD.sortByKey()
   sort.foreach(println)

    println()
    println()
    println("ReducedbyKey")
    println()
    val reduced = pairRDD.reduceByKey((a,b)=>a+b)
    reduced.foreach(println)

    def parm1=(accu:Int, v:Int) => accu + v
    def parm2=(accu1:Int, v2:Int) => accu1 + v2

    println()
    println()
    println("Aggregate  by key ==> wordcount")
  val wordcount2 = pairRDD.aggregateByKey(0)(parm1,parm2)
    println()
    wordcount2.foreach(println)

    //keys
    println()
    println()
    wordcount2.keys.foreach(println)

    //valores
    println()
    println()
    wordcount2.values.foreach(println)

    //Contar Palabras
    println()
    println()
   println( "Contar Palabras:" + wordcount2.count())

    //Collect as Map
    println()
    println()
    println("Collect As Map ==>")
    println()
    pairRDD.collectAsMap().foreach(println)

  }
}