package com.TarantulaStudios.spark.Covid

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.text.SimpleDateFormat

object CasosCovidFebrero {

  //Definimos los valores de sexo
  def sex(sexo: Int): String = {

    if (sexo == null) {
      return "No hay registros"

    } else if (sexo == 1) {
      return "Hombre"
    } else if (sexo == 2) {
      return "Mujer"
    } else {
      return "No hay registros"
    }

  }


  //Definimos los valores de personas embarazadas

  def emb(embarazo: Int): String = {
    if (embarazo == null) {
      return "No se hayaron registros"
    } else if (embarazo == 1) {
      return "Embarazada"
    } else if (embarazo == 2) {
      return "No embarazada"

    } else if (embarazo == 97) {
      return "No Aplica"

    } else if (embarazo == 98) {
      return "Se ignora"
    } else {
      return "No se hayaron registros"
    }
  }

  def pac(paciente: Int): String = {
    if (paciente == null) {
      return "No se hayaron registros"
    } else if (paciente == 1) {
      return "Ambulatorio"
    } else if (paciente == 2) {
      return "Hospitalizado"
    } else {
      return "No se hayaron registros"
    }
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Casos Covid 14 Febrero <3")
      .getOrCreate()

    import spark.implicits._

    val covidFebrero_df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:/Users/SDS-Usuario/Downloads/spark/data/data/covid19/220214COVID19MEXICO.csv")

    val dateFormatter = new SimpleDateFormat("dd/MM/yyyy")
    covidFebrero_df.printSchema()

    //Valores Sexo
    val sex = spark.udf.register("sex", (sexo: Int) => {

      if (sexo == null) {
        "No hay registros"

      } else if (sexo == 1) {
        "Hombre"
      } else if (sexo == 2) {
        "Mujer"
      } else {
        "No hay registros"
      }
    })

    val emb = spark.udf.register("emb", (embarazo: Int) => {

      if (embarazo == null) {
        "No se hayaron registros"
      } else if (embarazo == 1) {
        "Embarazada"
      } else if (embarazo == 2) {
        "No embarazada"

      } else if (embarazo == 97) {
        "No Aplica"

      } else if (embarazo == 98) {
        "Se ignora"
      } else {
        "No se hayaron registros"
      }
    })

    val pac = spark.udf.register("pac", (paciente: Int) => {

      if (paciente == null) {
        "No se hayaron registros"
      } else if (paciente == 1) {
        "Ambulatorio"
      } else if (paciente == 2) {
        "Hospitalizado"
      } else {
        "No se hayaron registros"
      }
    })


    //Casos confirmados Covid-19 al 14 de Febrero del 2022
    val casos_confirmados = covidFebrero_df.na.drop("any", Seq("CLASIFICACION_FINAL"))
      .filter((($"CLASIFICACION_FINAL" === 1) ||
        ($"CLASIFICACION_FINAL" === 2) ||
        ($"CLASIFICACION_FINAL" === 3)))
      .withColumn("TIPO_PACIENTE", pac($"TIPO_PACIENTE"))
      .select("EDAD",
        "SEXO",
        "ENTIDAD_RES",
        "FECHA_INGRESO",
        "FECHA_SINTOMAS",
        "FECHA_DEF",
        "EMBARAZO",
        "TIPO_PACIENTE",
        "MUNICIPIO_RES",
      )
    casos_confirmados.createOrReplaceTempView("COVID")


    val personas_covid_neza = spark.sql("SELECT EDAD, SEXO, ENTIDAD_RES, FECHA_INGRESO, FECHA_SINTOMAS, TIPO_PACIENTE, MUNICIPIO_RES FROM COVID WHERE ENTIDAD_RES=='15'  AND MUNICIPIO_RES=='058'  ORDER BY FECHA_INGRESO")
    //.withColumn("TIPO_PACIENTE", pac($"TIPO_PACIENTE"))


    val total_casos_analizados = covidFebrero_df.count()
    val total_personascon_covid = casos_confirmados.count()
    val total_covid_neza = personas_covid_neza.count()


    println("Total de registros analizados: " + total_casos_analizados)
    println("Total de Personas con Covid a la fecha: " + total_personascon_covid)
    println("Total de Personas que viven en Neza con Covid: " + total_covid_neza)

    personas_covid_neza.show()

    personas_covid_neza.sort($"FECHA_INGRESO".asc).repartition(1).write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv("C:/Users/SDS-Usuario/Downloads/spark/data/data/Casos Covid en Neza")

    spark.stop()
  }


}
