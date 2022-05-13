package com.TarantulaStudios.spark.SparkSQL

import org.apache.log4j.{Level, Logger}
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.text.SimpleDateFormat
import java.util.Calendar

object la_bandita {
  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("Edades_Greñas")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val greñasfriends_df = spark.read
      .option("Header", "True")
      .option("inferSchema","True")
      .csv("data/fakefriends.csv")

    val dateFormatter = new SimpleDateFormat("dd/MM/yyyy")
    greñasfriends_df.printSchema()

    val rango_compas =greñasfriends_df.na.drop("any", Seq("age"))
      .where($"age" >10 && $"age" <35)
      .select($"name",
        $"age",
        lit(dateFormatter.format(Calendar.getInstance().getTime())).as("FECHA_ACTUAL"))
      .orderBy($"age".asc)

    val greñas_name = greñasfriends_df.na.drop("any", Seq("name"))
      .filter((($"name" === "El Peluca Sape") || ($"name" === "El Grenas")))
      .select($"name",
        $"age")

    val total_rango_edad= rango_compas.count()
    val total_greñudos = greñas_name.count()

    println("Total de personas en un rango de 10 a 35 años: "+ total_rango_edad)
    println("Total de personas llamadas Greñas: " + total_greñudos)

    rango_compas.show(30)
    greñas_name.show(10)

    rango_compas.write.format("json")
      .mode(SaveMode.Overwrite)
      .save("C:/Users/SDS-Usuario/Downloads/spark/data/data/AmigosJson")
    spark.stop()
  }
}
