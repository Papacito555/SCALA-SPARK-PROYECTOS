package com.TarantulaStudios.spark.Parquet

import org.apache.spark.sql.{SaveMode, SparkSession}

object ParquetSalarios {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Parquet Hadoop")
      .getOrCreate()

    //Creacion Tabla

    val data = Seq(("Porfirio ", "", "El Pollo", "13452", "M", 2, 50000),
      ("Jacinta", "Mclovin", "Espinosa", "23452", "F", 4, 100000),
      ("Luis", "Crescencio", "Sandoval", "23513", "M", 61, 80000),
      ("Gilberto", "Antonio", "Resendiz", "134D32", "M", 50, 50000),
      ("Orlee", "Adina", "Espinosa", "23DEWS", "F", 17, 26000),
      ("Ana", "Laura", "Peña", "3DDES4", "F", 25, 35000),
      ("Margot", "", "Johansson", "3FDER4", "F", 31, 45000),
      ("Juan Luis", "Londono", "Arias", "45RFDE2", "M", 27, 90000),
      ("Trevor", "Crazy", "Philips", "4FFDER4", "M", 54, 55000),
      ("Scarlett", "", "Robbie", "65DDE3", "F", 35, 60000),
      ("Jacinta", "Mclovin", "Espinosa", "23452", "F", 4, 500000)
    )


    val columnas = Seq("Primer_Nombre", "Segundo_Nombre", "Apellido", "Matricula", "Genero", "Edad", "Salario")
    import spark.sqlContext.implicits._

    val df = data.toDF(columnas: _*)

    df.show()
    df.printSchema()

    //Escribir la Tabla en un archivo CSV
    /*
            df.write.format("csv")
              .mode(SaveMode.Overwrite)
              .save("C:/Users/INformatica/Documents/SCALA/SparkScala/data/Trabajadores")
          */


    df.write
      .mode(SaveMode.Overwrite)
      .parquet("C:/Users/SDS-Usuario/Downloads/spark/data/data/Trabajadores")


    val parqDF = spark.read.parquet("C:/Users/SDS-Usuario/Downloads/spark/data/data/Trabajadores")
    parqDF.createOrReplaceTempView("ParquetTable")

    //Esta Madre se utiliza solo si ya tienes un archivo escrito
    //var df1:DataFrame = spark.read.option("header","true").csv(parqDF)
    //Con esta linea se escribe nuestro data Frame en un solo archivo
    parqDF.repartition(1).write.option("header", "true").csv("C:/Users/SDS-Usuario/Downloads/spark/data/data/DatosMACO")

    spark.sql("Select * FROM ParquetTable where Salario >=50000").explain()
    val parkSQL = spark.sql("Select * FROM ParquetTable where Salario >=50000")

    parkSQL.show()
    parkSQL.printSchema()

    /*
      df.write
        .parquet("C:/Users/INformatica/Documents/SCALA/SparkScala/data/Trabajadoressalariomayora50") */

    val parqDF2 = spark.read.parquet("C:/Users/SDS-Usuario/Downloads/spark/data/data/Trabajadores")
    parqDF2.createOrReplaceTempView("ParquetTable2")

    val df3 = spark.sql("Select * FROM ParquetTable2 where Apellido = 'Espinosa'")
    df3.explain()
    df3.printSchema()
    df3.show()

    val ParqDF3 = spark.read.parquet("C:/Users/SDS-Usuario/Downloads/spark/data/data/Trabajadores")
    ParqDF3.createOrReplaceTempView("ParquetTable3")

    val df4 = spark.sql("Select  Primer_Nombre, SUM(Salario) FROM ParquetTable3 group by Primer_Nombre having SUM(Salario)>50000")
    df4.explain()
    df4.show()
    df4.printSchema()

    val ParqDF4 = spark.read.parquet("C:/Users/SDS-Usuario/Downloads/spark/data/data/Trabajadores")
    ParqDF4.createOrReplaceTempView("ParquetTable4")

    val df5 = spark.sql("SELECT Primer_Nombre, AVG(Salario) FROM ParquetTable4 group by Primer_Nombre having AVG(Salario)")
    // val df5 = spark.sql("SELECT Primer_Nombre, MAX(Salario) FROM ParquetTable4 group by Primer_Nombre having MAX(Salario)")

    df5.show()
    df5.explain()
    df5.printSchema()

    spark.stop()

  }


}
