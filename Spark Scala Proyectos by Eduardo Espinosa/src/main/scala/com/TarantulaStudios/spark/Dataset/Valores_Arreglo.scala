/*
package com.TarantulaStudios.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Valores_Arreglo extends App {
  val logFile = "OlympicAthletes.csv"
  val sc = new SparkContext("local", "Simple", "$SPARK_HOME"
    , List("target/spark-ejercicio-1.0.jar"))

  //Todo el fichero csv es cargado en este RDD
  val file = sc.textFile(logFile)

  //linea a linea se aplica un split o separacion utilizando la ","
  //eso nos arrojara un arreglo de palabras (o numeros)
  //y construimos una instancia del tipo OlympicMedalRecords
  val olympicMedalRecordsRDD = file.map(x =&gt; {
    val arr = x.split(",")
    new OlympicMedalRecords(arr(0), Integer.parseInt(arr(1)), arr(2)
      , Integer.parseInt(arr(3)), arr(5), Integer.parseInt(arr(6)),
      Integer.parseInt(arr(7)), Integer.parseInt(arr(8)))
  }
  )


  //Mis ejercicios
  //Ejercicio 1
  //mapeo del tipo (edad, num de medallas), val medal es un RDD
  val medal = olympicMedalRecordsRDD.map(record =&gt; (record.getAge, record.getGoldMedals + record.getSilverMedals + record.getBronzeMedals))

  //creamos un nuevo RDD el cual manejara la suma de todas las madellas por edad
  val sum = medal.reduceByKey((acc: Int,value: Int)=&gt;acc+value)
  println("Lista de Medallas ordenado por Edad")

  //ordenamos, juntamos y luego recorremos para mostrar todos los valores
  sum.sortBy(_._1).collect.foreach(println)


  //Ejercicio 2 hacer un ranking por atleta
  // ya que oro = 3 ptos, plata = 2ptos, bronce = 1pto
  //sportGuy es otro RDD y aqui hacemos un mapeo del tipo (nombre_atleta, puntos_por_medalla)
  val sportGuy = olympicMedalRecordsRDD.map(record =&gt; (record.getName, 3*record.getGoldMedals + 2*record.getSilverMedals + record.getBronzeMedals))

  //ranking es a su vez otro RDD  y de igual manera como se hizo en el ejemplo anterior se agrupa por nombre de atleta
  // y también utilizamos una variable acumuladora acc donde vamos sumando los ptos de un determinado atleta
  val ranking = sportGuy.reduceByKey((acc,value)=&gt;acc+value)
  println("Ranking de deportistas")
  // ordenamos en sentido inverso y mostramos  los atletas con mas ptos
  ranking.sortBy(_._2, false).collect.foreach(println)
}

 */