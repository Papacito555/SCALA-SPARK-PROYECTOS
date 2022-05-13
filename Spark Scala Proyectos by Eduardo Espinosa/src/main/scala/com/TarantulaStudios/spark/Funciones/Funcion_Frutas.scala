package com.TarantulaStudios.spark.Funciones

import org.apache.spark.sql.SparkSession

object Funcion_Frutas {

  def main(args: Array[String]) = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Frutas EL MACO")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val frutas = Array("Pera", "Manzana", "Pina", "Durazno", "Uva", "Aguacate", "Lichi", "Maracuya", "Fresa")
    frutas

    frutas.apply(0)
    frutas(1)
    frutas(3)
    frutas.length
    frutas.isEmpty
    frutas.nonEmpty
    frutas.indexOf("Pera")
    frutas.foreach(X => "Pera")
    /*
    //def suma(a: Int, b:Int, c:Int) = a+b+c
    val num_list = [1,2,3,4,5]
    num_list.remove(2)
    println(num_list)
    spark.stop()

 */
  }
}
