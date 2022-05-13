package com.TarantulaStudios.spark.RDD

  import org.apache.hadoop.conf.Configuration
  import org.apache.hadoop.fs._

  import org.apache.log4j._
  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._

  import java.text.SimpleDateFormat
  import java.util.Calendar
  import org.apache.spark.sql.SparkSession

  object CSVenunosolo {

    def main(args:Array[String]): Unit={

      val spark = SparkSession.builder()
        .master("local[*]")
        .appName("Primer RDD")
        .getOrCreate()

      spark.sparkContext.setLogLevel("ERROR")

      val rdd = spark.sparkContext.parallelize(

        List("Jacinta Porfirio Nolan", "Pinto_Madafaka Piojosa Serafin", "Jacinta Maximiliana Piojosa Natasha")
      )

      val listaRDD = spark.sparkContext.parallelize(List(12,5,8,9,12,5,6,8,20))

      println("Maximo: "+ listaRDD.reduce((a,b)=> a max b))
      println("Minimo: " + listaRDD.reduce((a,b)=>a min b))
      println("Suma: " + listaRDD.reduce((a,b)=> a+b))
      println("Resta: " + listaRDD.reduce((a,b) => a - b))
      println("Multiplicación: "+listaRDD.reduce((a,b) => a * b))

      /*
      println("División: " + listaRDD.reduce((a,b) => a / b))
      println("Promedio: "+ listaRDD.reduce((a,b) => a compareTo(b))) */

      /*
      val wordsRDD = rdd.flatMap(_.split(""))
      wordsRDD.foreach(println)

      print("Ordenar por Nombre")

      val sortRDD = wordsRDD.sortBy(f=>f)

      val agruparRDD = wordsRDD.groupBy(word=>word.length)
      agruparRDD.foreach(println)

      val tupp2RDD = wordsRDD.map(f=>(f,1))
      tupp2RDD.foreach(println)
      */

      //flatMap
      val wordsRdd = rdd.flatMap(_.split(" "))
      wordsRdd.foreach(println)

      //sortBy
      println("Ordenar por Nombre: ")
      val sortRdd = wordsRdd.sortBy(f=>f) // also can write f=>f

      //GroupBy
      val groupRdd = wordsRdd.groupBy(word=>word.length)
      groupRdd.foreach(println)

      //map
      val tupp2Rdd = wordsRdd.map(f=>(f,1))
      tupp2Rdd.foreach(println)

    }

  }

}
