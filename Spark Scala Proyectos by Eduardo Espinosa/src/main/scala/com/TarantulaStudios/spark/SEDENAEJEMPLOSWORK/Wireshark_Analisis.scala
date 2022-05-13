package com.TarantulaStudios.spark.SEDENAEJEMPLOSWORK


import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter, RightOuter}

object Wireshark_Analisis {

  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Wireshark Analisis")
      .getOrCreate()


    val WIFI = spark.read
      .option("Header", "True")
      .option("InferSchema", "True")
      .csv("C:/Users/SDS-Usuario/Downloads/spark/data/data/Paquetes enviados de mi PC.csv")


    WIFI.printSchema()
    WIFI.show()

    /*
        val protocolo_TCP = WIFI.na.drop("any",Seq("Protocol"))
          .filter("Protocol")
          .select("Protocol",
          "Source",
          "Destination",
          )

     */
    import spark.sqlContext.implicits._
    WIFI.createOrReplaceTempView("WIFI")
    val protocolo_TCP = spark.sql("SELECT Protocol, Source, Destination, Info FROM WIFI where Protocol =='TCP' AND INFO LIKE '%Seq=1%' ORDER BY Source")
    protocolo_TCP.printSchema()
    protocolo_TCP.show()

    //Exluir los protocolos ARP

    val exclusion_ARP = spark.sql("SELECT Protocol, Source, Destination FROM WIFI where Protocol not like '%ARP%'")
    exclusion_ARP.show()

    val total_Conexiones = WIFI.count()
    val total_conexiones_TCP = protocolo_TCP.count()

    println("Total de Transacciones hechas desde mi IP: " + total_Conexiones)
    println("Total de Conexiones usando el protocolo TCP: " + total_conexiones_TCP)


    spark.stop()
  }

}
