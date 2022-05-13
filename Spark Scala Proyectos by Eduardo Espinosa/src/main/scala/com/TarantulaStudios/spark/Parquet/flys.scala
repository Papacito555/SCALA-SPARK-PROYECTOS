package com.TarantulaStudios.spark.Parquet

import org.apache.spark.sql.SparkSession

object flys {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Analisis Datos Fondo Grafico Nacional")
      .getOrCreate()

    import spark.implicits._

    val datos = spark.read
      .option("header", "true")
      .option("InterSchema", "True")
      .csv("C:/Users/SDS-Usuario/Downloads/spark/data/data/INAH/FONDO_GRAFICO_EL_NACIONAL_2021.csv")

    val REGISTRO_PROCESO = datos.na.drop("any", Seq("anio_registro_sitio_web_fototeca_inehrm"))
      .filter($"anio_registro_sitio_web_fototeca_inehrm" === "EN PROCESO")
      .select($"num_expediente",
        $"nombre_del_personaje",
        $"profesion_o_actividad_del_personaje",
        $"fondo_documental",
        $"digitalizado_si_no",
        $"anio_registro_sitio_web_fototeca_inehrm"
      )

    datos.createOrReplaceTempView("DATOS")
    val Procesos = spark.sql("SELECT num_expediente, nombre_del_personaje, profesion_o_actividad_del_personaje, fondo_documental, digitalizado_si_no, anio_registro_sitio_web_fototeca_inehrm FROM DATOS Where anio_registro_sitio_web_fototeca_inehrm =='EN PROCESO' AND  profesion_o_actividad_del_personaje= 'Periodista' order by nombre_del_personaje")

    val EXPEDIENTES = spark.sql("SELECT num_expediente, nombre_del_personaje, profesion_o_actividad_del_personaje, fondo_documental FROM DATOS WHERE num_expediente LIKE 'A-%' order by nombre_del_personaje")

    val FECHAS_1908 = spark.sql("SELECT num_expediente, nombre_del_personaje, profesion_o_actividad_del_personaje, fondo_documental, fechas_extremas FROM DATOS WHERE fechas_extremas LIKE '%1908%' order by nombre_del_personaje")

    val APELLIDOS_CON_SEIS_LETRAS = spark.sql("SELECT num_expediente, nombre_del_personaje, profesion_o_actividad_del_personaje, fondo_documental FROM DATOS where nombre_del_personaje LIKE '______' order by nombre_del_personaje")

    val Filtro_letras = spark.sql("SELECT num_expediente, nombre_del_personaje, profesion_o_actividad_del_personaje, fondo_documental FROM DATOS where profesion_o_actividad_del_personaje LIKE '%I%g%e%' order by nombre_del_personaje")

    val total_obras_en_proceso = REGISTRO_PROCESO.count()
    val total_procesillos = Procesos.count()
    val total_obras_letra_A = EXPEDIENTES.count()
    val total_fechas_1908 = FECHAS_1908.count()
    val TOTAL_APELLIDOS_CON_SEIS_LETRAS = APELLIDOS_CON_SEIS_LETRAS.count()
    val Total_filtros_con_IGE = Filtro_letras.count()

    println("Total de Obras que se encuentran en proceso de Registro en sitio Web Fototeca: " + total_obras_en_proceso)
    println("Total de Obras que se encuentran en proceso de Registro y los personajes son periodistas: " + total_procesillos)
    println("Total de Obras con Expediente que inicia con la letra 'A': " + total_obras_letra_A)
    println("Total de Obras que en fechas extremas cuentan con 1908: " + total_fechas_1908)
    println("Total de Nombres con 6 Letras: " + TOTAL_APELLIDOS_CON_SEIS_LETRAS)
    println("Total de Personas con el Filtro: " + Total_filtros_con_IGE)

    println("Procesos Registro")
    REGISTRO_PROCESO.show()

    println("Periodistas")
    Procesos.show()

    println("Expediente Letra 'A'")
    EXPEDIENTES.show()

    println("Fechas 1908")
    FECHAS_1908.show()

    println("APELLIDOS CON 6 LETRAS")
    APELLIDOS_CON_SEIS_LETRAS.show()

    println("TOTAL DE PERSONAS CON EL FILTRO")
    Filtro_letras.show()

    spark.stop()
  }

}
