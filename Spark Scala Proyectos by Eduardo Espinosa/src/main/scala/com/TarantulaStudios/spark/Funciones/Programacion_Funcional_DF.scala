package com.TarantulaStudios.spark.Funciones

import org.apache.spark.sql.SparkSession

object Programacion_Funcional_DF {

  def main(args: Array[String]) = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("PROGRAMACION FUNCIONAL ")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    def saludo(name: String): Unit = {
      println("Hola bbsote " + name)
    }

    val datos = spark.read
      .option("header", "true")
      .option("InferSchema", "true")
      .csv("C:/Users/SDS-Usuario/Downloads/spark/data/data/SEDENA_DATA/papaciente.csv")
    datos.createOrReplaceTempView("DATOS")

    val paciente = spark.sql("SELECT papacientenombre, papacienteapellidopaterno, papacienteapellidomaterno FROM DATOS where papacienteexpediente ='GARUIAO090363' ")
    val paciente_charro = paciente.toString()
    paciente.show()
    val saludoFunc = saludo _
    saludoFunc("Guapo Ben")


  }
}
