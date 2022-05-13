package com.TarantulaStudios.spark.Parquet

import org.apache.spark.sql.{SaveMode, SparkSession}

object Parquet {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Parquet ")
      .getOrCreate()

    val data = Seq(("Antonio", "Garcia", "Ruiz", "General Brigadier", "GARA630309HDFRZN07", "M", "10:00-10:30", "genantoniogarcia@gmail.com", "Cirugía Plástica", "Subsecuente"),
      ("Gilberto", "Antonio", "Resendiz", "Teniente Coronel", "AORG720204465", "M", "11:00-11:30", "garesendiz@gmail.com", "Cirugía Vascular", "Primera Vez"),
      ("Silvia", "De la Cruz", "Faustino", "Cabo", "CUFS861130MDFRSL03", "F", "08:00-08:30", "chiborras13@gmail.com", "Oncologia", "Renovacion de Receta"),
      ("Dolores", "Montalvo", "Correa", "General de División", "MOCD680310MPLNRL07", "F", "09:00-09:30", "ea5007_49@hotmail.com", "Cirugia Vascular", "Primera Vez"),
      ("Luis Cresencio", "Sandoval", "Gonzalez", "General Secretario", "SAGL600207HBCNNS09", "M", "13:30-14:00", "gralluissandoval@sedena.gob.mx", "Cirugia de Hombres", "Primera Vez"),
      ("Andres Manuel", "Lopez", "Obrador", "Comandante Supremo", "LOOA531113HTBPBN02", "M", "12:00-12:30", "elpeje@gob.mx", "Urologia", "Subsecuente"),
      ("Zenon Eduardo", "Espinosa", "Y Sanchez", "General de Brigada", "EISZ430623HPLSNN00", "M", "13:00-13:30", "elreysupremob@hotmail.com", "Urologia", "Renovacion de Receta"),
      ("Porfirio", "El Pollo", "", "Coronel", "EOEP201101G37", "M", "11:30-12:00", "porfirioelpollo@gmail.com", "Cirugía de Pollos", "Primera Vez")
    )

    val columnas = Seq("Nombre", "Ap", "Am", "Rango", "CURP", "Sexo", "Horario", "correo", "Especialidad", "Valoracion")
    import spark.sqlContext.implicits._

    val df = data.toDF(columnas: _*)

    df.show()
    df.printSchema()
    /*
    df.write.format("csv")
      .mode(SaveMode.Overwrite)
      .save("C:/Users/SDS-Usuario/Downloads/spark/data/data/CitasHCM") */
    df.write
      .mode(SaveMode.Overwrite)
      .parquet("C:/Users/SDS-Usuario/Downloads/spark/data/data/CitasHCM")

    val parqDF = spark.read.parquet("C:/Users/SDS-Usuario/Downloads/spark/data/data/CitasHCM")
    parqDF.createOrReplaceTempView("ParquetTable")

    spark.sql("Select * FROM ParquetTable where Valoracion = 'Primera Vez'").explain()
    val ParkSQL = spark.sql("Select * FROM ParquetTable where Valoracion = 'Primera Vez' order by Ap")
    ParkSQL.show()
    ParkSQL.printSchema()
    ParkSQL.write.format("csv")
      .mode(SaveMode.Overwrite)
      .save("C:/Users/SDS-Usuario/Downloads/spark/data/data/PrimeraVezHCM")


    val parkDF2 = spark.read.parquet("C:/Users/SDS-Usuario/Downloads/spark/data/data/CitasHCM")
    parkDF2.createOrReplaceTempView("ParquetTable2")
    val df2 = spark.sql("SELECT Sexo, Count(*) from ParquetTable2  group  by Sexo having(Sexo= 'M')")
    //val df2 = spark.sql("SELECT  Sexo, Count(*) as Sexo, Nombre, FROM ParquetTable2 group by Sexo having (Sexo= 'M') ")
    //val df2 = spark.sql("SELECT DISTINCT COUNT(Sexo) OVER(PARTITION BY Nombre) AS Nombre, Nombre FROM ParquetTable2 WHERE Sexo = 'M' ")
    df2.show()
    df2.printSchema()
    df2.write.format("JSON")
      .mode(SaveMode.Overwrite)
      .save("C:/Users/SDS-Usuario/Downloads/spark/data/data/PrimeraVezHCM")


    val parkDF3 = spark.read.parquet("C:/Users/SDS-Usuario/Downloads/spark/data/data/CitasHCM")
    parkDF3.createOrReplaceTempView("ParquetTable3")

    // val df3 = spark.sql()
  }
}
