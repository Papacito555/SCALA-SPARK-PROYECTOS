package com.TarantulaStudios.spark.Covid

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{date_format, lit, to_date}

import java.text.SimpleDateFormat
import java.util.Calendar

object CovidCase {

  def sex(sexo: Int): String = {
    if (sexo == null) {
      return "sin sexo"
    } else if (sexo == 1) {
      return "hombre"
    } else if (sexo == 2) {
      return "mujer"
    } else {
      return "sin sexo"
    }

  }

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("Covid_Cases")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val covid_df = spark.read
      .option("header", "True")
      .option("inferSchema", "True")
      .csv("C:/Users/SDS-Usuario/Downloads/spark/data/data/covid19/211125COVID19MEXICO.csv")
    val dateFormatter = new SimpleDateFormat("dd/MM/yyyy")
    covid_df.printSchema()


    val sex = spark.udf.register("sex", (sexo: Int) => {
      if (sexo == null) {
        "sin sexo"
      } else if (sexo == 1) {
        "hombre"
      } else if (sexo == 2) {
        "mujer"
      } else {
        "sin sexo"
      }
    })
    val covid_confirmed_cases = covid_df.na.drop("any", Seq("CLASIFICACION_FINAL"))
      .filter((($"CLASIFICACION_FINAL" === 1) ||
        ($"CLASIFICACION_FINAL" === 2) ||
        ($"CLASIFICACION_FINAL" === 3)))
      .select("EDAD",
        "SEXO",
        "ENTIDAD_RES",
        "FECHA_INGRESO",
        "FECHA_SINTOMAS",
        "FECHA_DEF")
    val covid_dead_confirmed_cases = covid_confirmed_cases.na.drop("any", Seq("FECHA_DEF"))
      .filter($"FECHA_DEF" =!= "9999-99-99")
      .withColumn("SEXO", sex($"SEXO"))
      .select($"SEXO",
        $"ENTIDAD_RES",
        date_format(to_date($"FECHA_INGRESO", "yyyy-MM-dd"), "dd-MM-yyyy").as("FECHA_INGRESO"),
        date_format(to_date($"FECHA_SINTOMAS", "yyyy-MM-dd"), "dd-MM-yyyy").as("FECHA_SINTOMAS"),
        date_format(to_date($"FECHA_DEF", "yyyy-MM-dd"), "dd-MM-yyyy").as("FECHA_DEF"),
        lit(dateFormatter.format(Calendar.getInstance().getTime())).as("FECHA_ACTUAL"))
    val total_records_analized = covid_df.count()
    val total_records_with_covid_confirmed = covid_confirmed_cases.count()
    val total_records_dead_covid_dead_confirmed_cases = covid_dead_confirmed_cases.count()

    println("Total de caso analizados = " + total_records_analized)
    println("Total de caso covid confirmados = " + total_records_with_covid_confirmed)
    println("Total de caso covid fallecidos= " + total_records_dead_covid_dead_confirmed_cases)

    covid_dead_confirmed_cases.show(10)


    spark.stop()

  }

}
