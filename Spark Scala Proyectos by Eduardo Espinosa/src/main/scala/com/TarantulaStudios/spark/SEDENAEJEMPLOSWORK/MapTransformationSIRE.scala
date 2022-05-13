package com.TarantulaStudios.spark.SEDENAEJEMPLOSWORK

import org.apache.spark.sql.{SaveMode, SparkSession}

object MapTransformationSIRE {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Transformacion de Mapas")
      .getOrCreate()

    val datos = spark.read
      .option("Header", "True")
      .option("InferSchema", "True")
      .csv("C:/Users/SDS-Usuario/Downloads/spark/data/data/17MAR-COVID.csv")
    datos.printSchema()
    datos.show(false)

    import spark.implicits._

    val mapa = datos.map(row => {

      val Nombre_Completo = row.getString(0) + " " + row.getString(1) + " " + row.getString(2)
      (Nombre_Completo)
    })

    val df1 = mapa.toDF("Nombre_Completo")

    df1.printSchema()
    df1.show(false)

    df1.repartition(1).write
      .option("header", "True")
      .mode(SaveMode.Overwrite)
      .csv("C:/Users/SDS-Usuario/Downloads/spark/data/data/SIRE COMPLETO")


  }
}
