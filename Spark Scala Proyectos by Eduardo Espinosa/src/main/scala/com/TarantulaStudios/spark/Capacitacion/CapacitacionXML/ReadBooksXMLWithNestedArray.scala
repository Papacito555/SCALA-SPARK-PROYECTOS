package com.TarantulaStudios.spark.Capacitacion.CapacitacionXML

import com.TarantulaStudios.spark.Clases.Libros
import  com.TarantulaStudios.spark.Clases.LibrosconArreglo
import  com.TarantulaStudios.spark.Clases.Tiendas
import org.apache.spark.sql.{SparkSession, functions}

object ReadBooksXMLWithNestedArray {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    import spark.implicits._
    val ds = spark.sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "book")
      .load("src/main/resources/books_withnested_array.xml").as[LibrosconArreglo]

    ds.printSchema()
    ds.show()
/*
    ds.foreach(f=>{
      println(f.author+","+f.OtherInfo.country+","+f.OtherInfo.address.addressline1)
      for(s<-f.Tiendas.store){
        println(s.name)
      }

    })
    */


  }
}

