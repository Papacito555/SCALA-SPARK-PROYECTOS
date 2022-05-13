package com.TarantulaStudios.spark.SparkSQL


import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object Joins_Charros{

  def main(args:Array[String]): Unit ={

    val spark =SparkSession.builder()
      .master("local[*]")
      .appName("Joins")
      .getOrCreate()

    import spark.sqlContext.implicits._

    //Prueba Joins con Comandos SQL
    val df = Seq(
      (1,"Paco","Garcia",24,24000),
      (2,"Juan","Garcia",26,27000),
      (3,"Lola","Martin",29,31000),
      (4,"Sara","Garcia",35,34000)
    ).toDF("id","nombre", "apellido","edad","salario")

    df.createOrReplaceTempView("personas")

    val df2 = Seq(
      (1,"Rojo","Pasta"),
      (2,"Amarillo","Pizza"),
      (3,"Azul","Patatas"),
      (5,"Rojo","Pizza"),
      (6,"Negro","Pulpo")
    )
      .toDF("id", "color","comida")

    df2.createOrReplaceTempView("gustos")

    spark.sql(
      """select p.*, g.*
        |from personas p
        |inner join gustos g
        |on p.id = g.id
    """.stripMargin)
      .show

    spark
      .sql(
        """select p.*, g.*
          |from personas p
          |left outer join gustos g
          |on p.id = g.id
    """.stripMargin)
      .show

    spark
      .sql(
        """select p.*, g.*
          |from personas p
          |right outer join gustos g
          |on p.id = g.id
    """.stripMargin)
      .show

    spark.sql("SELECT nombre, apellido,edad, comida FROM personas p left join gustos g ON p.id=g.id where apellido == 'Garcia' order by nombre")
      .show()
  }

}

