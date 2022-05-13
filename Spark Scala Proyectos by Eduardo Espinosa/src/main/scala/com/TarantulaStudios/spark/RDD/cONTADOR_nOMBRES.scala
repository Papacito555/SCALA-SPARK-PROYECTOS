package com.TarantulaStudios.spark.RDD

import org.apache.spark.sql.{SaveMode, SparkSession}

object cONTADOR_nOMBRES {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Transformacion de Mapas")
      .getOrCreate()

    val datos = spark.read
      .option("Header", "True")
      .option("InferSchema", "True")
      .csv("C:/Users/SDS-Usuario/Downloads/spark/data/data/cOMPARACION.csv")
    datos.printSchema()
    datos.show(false)


    datos.createOrReplaceTempView("DATOS")

    // val repetidos = spark.sql("SELECT NOMBRE_CHECAR FROM DATOS  GROUP BY NOMBRE_CHECAR  HAVING COUNT(*)>1")
    val repetidos = spark.sql("SELECT NOMBRE_CHECAR, NOMBRE_SIRE, COUNT(*) FROM DATOS GROUP BY NOMBRE_CHECAR, NOMBRE_SIRE HAVING COUNT(*)>1 ORDER BY NOMBRE_CHECAR, NOMBRE_SIRE ")
    repetidos.show()
    val total = repetidos.count()

    println("Total Reptidos: " + total)

    repetidos.repartition(1).write
      .option("Header", "True")
      .mode(SaveMode.Overwrite)
      .csv("C:/Users/SDS-Usuario/Downloads/spark/data/data/comparaidenticos")


    val repetidos_12 = spark.sql("SELECT COUNT(*) FROM DATOS  WHERE NOMBRE_CHECAR  IN (SELECT NOMBRE_CHECAR, NOMBRE_SIRE FROM DATOS  GROUP BY NOMBRE_CHECAR HAVING COUNT(*)>1)")
    repetidos_12.show()

    val total_rep = repetidos_12.count()
    println("Total Repetidos: " + total_rep)

  }
}
