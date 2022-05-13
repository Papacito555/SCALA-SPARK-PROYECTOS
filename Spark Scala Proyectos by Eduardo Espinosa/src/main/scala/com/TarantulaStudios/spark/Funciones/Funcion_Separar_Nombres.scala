package com.TarantulaStudios.spark.Funciones

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array_join, col, slice, split}

object Funcion_Separar_Nombres extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("Separar Datos")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val SimpleData = Seq(
    ("1", "1", "2", "Ford, F-150", "Listo", 230000),
    ("2", "1", "2", "Saleen, S-302 EXTREME", "Listo", 1000000),
    ("3", "2", "1", "Ford, Mustang", "Espera", 1125000),
    ("4", "3", "3", "Toyota, Prius", "Revision Aduana", 300000),
    ("5", "1", "2", "Mitsubishi, Lancer Evolution IX", "Listo", 500000),
    ("6", "2", "1", "Nissan, Skyline STR", "Espera", 1000000),
    ("7", "3", "3", "Lamborghini, Centenario", "Papeleo con Aduana", 10000000),
    ("8", "1", "2", "Lotus, Elise", "Validacion pago", 1300000),
    ("9", "2", "1", "Dodge, Charger SRT 2022", "Espera", 1750000),
    ("10", "3", "3", "Mclaren, 720-S", "Afinando Detalles Esteticos", 2125000),
    ("11", "1", "2", "Porche, 911", "Listo", 2000000),
    ("12", "2", "1", "Kawasaki, Ninja ZX-6R", "Espera", 150000),
    ("13", "3", "3", "Chevrolet, Camaro 2022", "Listo", 950000),
    ("14", "1", "2", "Dodge, Viper SRT-10", "Revision Aduana", 3000000),
    ("15", "2", "1", "Lamborghini, Huracan", "Espera", 5000000),
    ("16", "3", "3", "Ferrari, 458 Italia", "Revision Aduana", 4500000),
    ("17", "1", "2", "Mclaren, P1", "Listo", 15000000),
    ("18", "2", "1", "Ford, GT 40", "Espera", 7000000),
    ("19", "3", "3", "Tesla, Roadster 2021", "Revision Aduana", 964000),
    ("20", "1", "2", "Saleen, S7", "Listo", 6500000),
    ("21", "2", "1", "Lamborghini, Murcielago", "Espera", 11000000),
    ("22", "3", "3", "Aston Martin, DB11", "Listo", 2850000),
    ("23", "1", "2", "Mercedes Benz, SLK 200", "Listo", 3400000),
    ("24", "2", "1", "Chevrolet, Corvette Z06", "Papeleo con Aduana", 2000000),
    ("25", "3", "3", "Mclaren, 570S", "Papeleo con Aduana", 2500000)
  )

  val df = SimpleData.toDF("Id", "Id_Cliente", "Id_Empleado", "Vehiculo", "Estado_Importacion", "Precio")
  df.show(25)
  df.printSchema()

  val df2 = df.select(split(col("Vehiculo"), ",").getItem(0).as("Fabricante"),
    split(col("Vehiculo"), ",").getItem(1).as("Modelo"))
    .drop("Vehiculo")
  df2.show(false)

  val splitDF = df.withColumn("Fabricante", split(col("Vehiculo"), ",").getItem(0))
    .withColumn("Modelo", split(col("Vehiculo"), ",").getItem((1)))
    .drop("Vehiculo")
  splitDF.printSchema()
  splitDF.show(false)

  //3er Método  esta madre " \ "se hace así alt+92

  df.createOrReplaceTempView("COCHES")

  val Separa = spark.sql("SELECT Id, Split (Vehiculo,',') as Nave, Precio FROM COCHES ")
  Separa.show()

  val splitDF2 = df.withColumn("Fabricante", split(col("Vehiculo"), ",").getItem(0))
    .withColumn("Modelo", array_join(slice(split(col("Vehiculo"), ","), 2, 3), "/"))
  splitDF2.show(false)

}
