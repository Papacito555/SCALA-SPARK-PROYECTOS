package com.TarantulaStudios.spark.RDD


import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter, RightOuter}

object Pacientes_Negativos_Separar {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    // Crear un DataFrame desde la memoria
    import spark.implicits._
    val residentes = spark.read
      .option("Header", "True")
      .option("InferSchema", "True")
      .csv("C:/Users/SDS-Usuario/Downloads/spark/data/data/Pacientes Negativos .csv")


    // Método 1: use la función integrada dividir y luego iterar a través de las columnas
    val separator = " "
    lazy val first = residentes.first()

    val numAttrs = first.toString().split(separator).length
    val attrs = Array.tabulate(numAttrs)(n => "Nombres " + n)
    // Divide la columna de valor por el delimitador especificado para generar la columna splitCols
    var newDF = residentes.withColumn("splitCols", split($"Nombre", separator))
    attrs.zipWithIndex.foreach(x => {
      newDF = newDF.withColumn(x._1, $"splitCols".getItem(x._2))
    })
    newDF.show()

    newDF.createOrReplaceTempView("COVID")
    val todos = spark.sql("SELECT * FROM COVID")
    todos.show()

    todos.repartition(1).write
      .option("Header", "True")
      .mode(SaveMode.Overwrite)
      .json("C:/Users/SDS-Usuario/Downloads/spark/data/data/Pacientes_Negativos")


    residentes.createOrReplaceTempView("COVID")

    val repetidos = spark.sql("SELECT Nombre FROM COVID  GROUP BY Nombre  HAVING COUNT(*)>1")

    repetidos.show()
    val total = repetidos.count()

    println("Total Repetidos: " + total)


    val repetidos_12 = spark.sql("SELECT COUNT(*) FROM COVID  WHERE Nombre IN (SELECT Nombre FROM COVID  GROUP BY Nombre HAVING COUNT(*)>1)")
    repetidos_12.show()

    val total_rep = repetidos_12.count()
    println("Total Repetidos: " + total_rep)

    repetidos.sort($"Nombre").repartition(1).write
      .option("Header", "True")
      .mode(SaveMode.Overwrite)
      .json("C:/Users/SDS-Usuario/Downloads/spark/data/data/Pacientes_Negativos/Repetidos")


  }
}
