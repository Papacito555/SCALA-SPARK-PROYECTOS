package com.TarantulaStudios.spark.Covid

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{date_format, lit, to_date}

import java.text.SimpleDateFormat
import java.util.Calendar

object CasosCovid2022 {

  def sex(sexo: Int): String = {
    if (sexo == null) {
      return "No  Especificado"
    } else if (sexo == 1) {
      return "Hombre"
    } else if (sexo == 2) {
      return "Mujer"
    } else {
      return "No Especificado"
    }
  }

  def emb(embarazo: Int): String = {
    if (embarazo == null) {
      return "No hay Registros"
    } else if (embarazo == 1) {
      return "Embarazada"
    } else if (embarazo == 2) {
      return "No Embarazada"
    } else if (embarazo == 97) {
      return "No Aplica"
    } else if (embarazo == 98) {
      return "Se ignora"
    } else {
      return "No hay Registros"
    }
  }

  def pac(paciente: Int): String = {
    if (paciente == null) {
      return "No hay Registros"
    } else if (paciente == 1) {
      return "Ambulatorio"
    } else if (paciente == 2) {
      return "Hospitalizado"
    } else {
      return "No hay Registros"
    }
  }


  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("Covid 24/01/2022")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val covid2022_df = spark.read
      .option("header", "True")
      .option("inferSchema", "True")
      .csv("C:/Users/SDS-Usuario/Downloads/spark/data/data/covid19/220124COVID19MEXICO.csv")
    val dateFormatter = new SimpleDateFormat("dd/MM/yyyy")
    covid2022_df.printSchema()

    //Valores Sexo
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

    //Valores Paciente
    val pac = spark.udf.register("pac", (paciente: Int) => {
      if (paciente == null) {
        "No hay Registros"
      } else if (paciente == 1) {
        "Ambulatorio"
      } else if (paciente == 2) {
        "Hospitalizado"
      } else {
        "No hay Registros"
      }
    })

    //Valores Embarazo
    val emb = spark.udf.register("emb", (embarazo: Int) => {
      if (embarazo == null) {
        "No hay Registros"
      } else if (embarazo == 1) {
        "Embarazada"
      } else if (embarazo == 2) {
        "No Embarazada"
      } else if (embarazo == 97) {
        "No Aplica"
      } else if (embarazo == 98) {
        "Se ignora"
      } else {
        "No hay Registros"
      }
    })


    //Casos Confirmados Covid-19 al 24 de Enero del 2022
    val casos_confirmados = covid2022_df.na.drop("any", Seq("CLASIFICACION_FINAL"))
      .filter((($"CLASIFICACION_FINAL" === 1) ||
        ($"CLASIFICACION_FINAL" === 2) ||
        ($"CLASIFICACION_FINAL" === 3)))
      .select("EDAD",
        "SEXO",
        "ENTIDAD_RES",
        "FECHA_INGRESO",
        "FECHA_SINTOMAS",
        "FECHA_DEF",
        "EMBARAZO",
        "TIPO_PACIENTE",
        "MUNICIPIO_RES")

    //Defunciones Confirmadas por Covid-19 hasta el 24 de Enero del 2022
    val defunciones_confirmadas = casos_confirmados.na.drop("any", Seq("FECHA_DEF"))
      .filter($"FECHA_DEF" =!= "9999-99-99")
      .withColumn("SEXO", sex($"SEXO"))
      .select($"SEXO",
        $"ENTIDAD_RES",
        date_format(to_date($"FECHA_INGRESO", "yyyy-MM-dd"), "dd-MM-yyyy").as("FECHA_INGRESO"),
        date_format(to_date($"FECHA_SINTOMAS", "yyyy-MM-dd"), "dd-MM-yyyy").as("FECHA_SINTOMAS"),
        date_format(to_date($"FECHA_DEF", "yyyy-MM-dd"), "dd-MM-yyyy").as("FECHA_DEF"),
        lit(dateFormatter.format(Calendar.getInstance().getTime())).as("FECHA_ACTUAL"))


    //Pacientes que libraron el covid hasta el 24 de Enero del 2022
    val pacientes_vivos = casos_confirmados.na.drop("any", Seq("Fecha_DEF"))
      .filter($"FECHA_DEF" === "9999-99-99")
      .withColumn("SEXO", sex($"SEXO"))
      .select($"SEXO",
        $"ENTIDAD_RES",
        date_format(to_date($"FECHA_INGRESO", "yyyy-MM-dd"), "dd-MM-yyyy").as("FECHA_INGRESO"),
        date_format(to_date($"FECHA_SINTOMAS", "yyyy-MM-dd"), "dd-MM-yyyy").as("FECHA_SINTOMAS"),
        date_format(to_date($"FECHA_DEF", "yyyy-MM-dd"), "dd-MM-yyyy").as("FECHA_DEF"),
        lit(dateFormatter.format(Calendar.getInstance().getTime())).as("FECHA_ACTUAL"))

    //Pacientes Embarazadas que contrajeron Covid-19
    val pacientes_embarazadas = casos_confirmados.na.drop("any", Seq("EMBARAZO"))
      .filter($"EMBARAZO" === 1)
      .withColumn("EMBARAZO", emb($"EMBARAZO"))
      .withColumn("TIPO_PACIENTE", pac($"TIPO_PACIENTE"))
      .select($"EMBARAZO",
        $"ENTIDAD_RES",
        $"TIPO_PACIENTE",
        date_format(to_date($"FECHA_INGRESO", "yyyy-MM-dd"), "dd-MM-yyyy").as("FECHA_INGRESO"),
        date_format(to_date($"FECHA_SINTOMAS", "yyyy-MM-dd"), "dd-MM-yyyy").as("FECHA_SINTOMAS"),
        lit(dateFormatter.format(Calendar.getInstance().getTime())).as("FECHA_ACTUAL"))

    //Pacientes con Covid 19 que estan o estuvieron hospitalizados al 24 de Enero del 2022

    val hospitalizados_covid = casos_confirmados.na.drop("any", Seq("TIPO_PACIENTE"))
      .filter($"TIPO_PACIENTE" === 2)
      .withColumn("TIPO_PACIENTE", pac($"TIPO_PACIENTE"))
      .withColumn("SEXO", sex($"SEXO"))
      .select(date_format(to_date($"FECHA_INGRESO", "yyyy-MM-dd"), "dd-MM-yyyy").as("FECHA_INGRESO"),
        date_format(to_date($"FECHA_SINTOMAS", "yyyy-MM-dd"), "dd-MM-yyyy").as("FECHA_SINTOMAS"),
        $"SEXO",
        $"TIPO_PACIENTE",
        lit(dateFormatter.format(Calendar.getInstance().getTime())).as("FECHA_ACTUAL"))


    val total_records_analized = covid2022_df.count()
    val total_records_with_covid_confirmed = casos_confirmados.count()
    val total_records_dead_covid_dead_confirmed_cases = defunciones_confirmadas.count()
    val total_registros_pacientes_vivos = pacientes_vivos.count()
    val total_embarazadas_con_covid = pacientes_embarazadas.count()
    val total_de_personas_hospitalizadas_por_el_covid_19 = hospitalizados_covid.count()


    println("Total de caso analizados: " + total_records_analized)
    println("Total de caso covid confirmados: " + total_records_with_covid_confirmed)
    println("Total de caso covid fallecidos: " + total_records_dead_covid_dead_confirmed_cases)
    println("Total de Pacientes que libraron el Covid: " + total_registros_pacientes_vivos)
    println("Total de Mujeres Embarazadas con Covid: " + total_embarazadas_con_covid)
    println("Total de Pacientes que cayeron al Hospital por Covid-19: " + total_de_personas_hospitalizadas_por_el_covid_19)

    defunciones_confirmadas.show(10)
    pacientes_vivos.show(10)
    pacientes_embarazadas.show(10)
    hospitalizados_covid.show(10)

    /*readme = sc.textFile("README.md")

    wordCounts = readme.flatMap(lambda line: line.split()).map(lambda word: (word,
      1)).reduceByKey(lambda a, b: a+b)
    wordCounts.takeOrdered(1, key = lambda x: -x[1])
*/

    spark.stop()


  }

}
