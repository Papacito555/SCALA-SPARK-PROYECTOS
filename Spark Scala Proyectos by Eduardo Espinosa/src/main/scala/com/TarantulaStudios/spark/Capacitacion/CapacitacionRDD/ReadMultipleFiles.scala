package com.TarantulaStudios.spark.Capacitacion.CapacitacionRDD

object ReadMultipleFiles extends App {

  import org.apache.spark.sql.SparkSession

  object ReadMultipleFiles extends App {

    val spark:SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("Leer Multiples Archivos")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("leer todos los archivos de texto de un directorio a un solo RDD")
    val rdd = spark.sparkContext.textFile("C:/tmp/files/*")
    rdd.foreach(f=>{
      println(f)
    })

    println("lee todo")
    val rdd2 = spark.sparkContext.textFile("C:/tmp/files/text*.txt")
    rdd2.foreach(f=>{
      println(f)
    })

    println("sigue leyendo")
    val rdd3 = spark.sparkContext.textFile("C:/tmp/files/text01.txt,C:/tmp/files/text02.txt")
    rdd3.foreach(f=>{
      println(f)
    })

    println("y dale con la lectura")
    val rdd4 = spark.sparkContext.textFile("C:/tmp/files/text01.txt,C:/tmp/files/text02.txt,C:/tmp/files/*")
    rdd4.foreach(f=>{
      println(f)
    })


    val rddWhole = spark.sparkContext.wholeTextFiles("C:/tmp/files/*")
    rddWhole.foreach(f=>{
      println(f._1+"=>"+f._2)
    })

    val rdd5 = spark.sparkContext.textFile("C:/tmp/files/*")
    val rdd6 = rdd5.map(f=>{
      f.split(",")
    })

    rdd6.foreach(f => {
      println("Col1:"+f(0)+",Col2:"+f(1))
    })

  }
}
