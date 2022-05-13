package com.TarantulaStudios.spark.SEDENAEJEMPLOSWORK

import org.apache.spark.sql.{SaveMode, SparkSession}

object logs_Filezilla {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("SESIONES FILEZILLA")
      .getOrCreate()

    val read = spark.read
      .option("Header", "True")
      .text("C:/Users/SDS-Usuario/Downloads/spark/data/data/fzs-2022-03-03.log")

    read.show()


    read.repartition(1).write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv("C:/Users/SDS-Usuario/Downloads/spark/data/data/LoggeosFZ")

    read.createOrReplaceTempView("LOGEOS")

    val logeos_impresoras = spark.sql("SELECT * FROM LOGEOS WHERE value like '%Connected%'")
    logeos_impresoras.show()


    val total_filtro = read.count()
    val total_loggeados = logeos_impresoras.count()

    println("Total de conexiones filtradas con el valor 215268: " + total_filtro)
    println("Total de conexiones durante el día: " + total_loggeados)


    logeos_impresoras.repartition(1).write
      .option("Header", "True")
      .mode(SaveMode.Overwrite)
      .csv("C:/Users/SDS-Usuario/Downloads/spark/data/data/LoggeosFZ/Conexiones Exitosas")
  }

}
