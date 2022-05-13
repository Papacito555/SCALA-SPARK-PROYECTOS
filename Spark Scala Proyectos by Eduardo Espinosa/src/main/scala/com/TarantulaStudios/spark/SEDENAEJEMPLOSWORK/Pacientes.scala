package com.TarantulaStudios.spark.SEDENAEJEMPLOSWORK


import com.sun.crypto.provider.PBEWithMD5AndTripleDESCipher
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter, RightOuter}

object Pacientes {

  def main(args:Array[String]): Unit= {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("bLOOD TYPE")
      .getOrCreate()

    val papacientemilitar = spark.read
      .option("header", "true")
      .option("Interschema", "True")
    //.csv("C:/CONFIDENCIAL")


    import spark.sqlContext.implicits._

   // militar.createOrReplaceTempView("MILITARES")

    val milicia = spark.sql("SELECT * FROM MILITARES")
    milicia.show()

    val tiposangre = spark.sql("SELECT * FROM MILITARES WHERE sangregrupo != 'null'")
    tiposangre.show()

    val neto = spark.sql("SELECT * FROM MILITARES WHERE expediente = 'CONFIDENCIAL' ")
    neto.show()

    val total_milicia = milicia.count()

    println("Total de Pacientes Militares: " + total_milicia)

  }
}
