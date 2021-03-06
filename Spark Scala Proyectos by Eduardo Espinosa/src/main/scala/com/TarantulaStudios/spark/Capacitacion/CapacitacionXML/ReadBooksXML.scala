package com.TarantulaStudios.spark.Capacitacion.CapacitacionXML

import com.TarantulaStudios.spark.Clases.LibrosDescuento
import com.TarantulaStudios.spark.Clases.Libros
import org.apache.spark.sql.{Encoders, SparkSession}

object ReadBooksXML {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    import spark.implicits._

    val ds = spark.sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "book")
      .load("src/main/resources/books.xml").as[Libros]


    val newds = ds.map(f=>{
      LibrosDescuento(f._id,f.author,f.description,f.price,f.publish_date,f.title, f.price - f.price*20/100)
    })

    newds.printSchema()
    newds.show()

    newds.foreach(f=>{
      println("Price :"+f.price + ", Discounted Price :"+f.discountPrice)
    })

    //First element
    println("First Element" +newds.first()._id)

  }
}


