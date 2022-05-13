package com.TarantulaStudios.spark.OperacionesAritmeticas

  import org.apache.spark.sql.SparkSession

  object LeerBinarios extends App {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    //spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read.format("binaryFile").load("C:/Users/SDS-Usuario/Downloads/spark/data/data/maco.png")
    df.printSchema()
    df.show()

    val df2 = spark.read.format("binaryFile").load("C:/Users/SDS-Usuario/Downloads/spark/data/data/")
    df2.printSchema()
    //df2.show(false)

    val df3 = spark.read.format("binaryFile").load("C:/Users/SDS-Usuario/Downloads/spark/data/data/*.png")
    df3.printSchema()
    df3.show(false)

    // To load files with paths matching a given glob pattern while keeping the behavior of partition discovery
    val df4 = spark.read.format("binaryFile")
      .option("pathGlobFilter", "*.png")
      .load("C:/Users/SDS-Usuario/Downloads/spark/data/data/")
    df4.printSchema()
    //df4.show(false)



}
