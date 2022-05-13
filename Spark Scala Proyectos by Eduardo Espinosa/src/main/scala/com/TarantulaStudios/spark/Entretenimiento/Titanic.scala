package com.TarantulaStudios.spark.Entretenimiento

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Titanic {

  //Sobrevivientes Titanic
  def sur(sobreviviente: Int): String = {

    if (sobreviviente == null) {
      return "No hay Registros"
    } else if (sobreviviente == 1) {
      return "Sobrevivio"
    } else if (sobreviviente == 0) {
      return "Fallecido"

    } else {
      return "No hay Registros Validos"
    }
  }

  //Clase Social Personas
  def soc(social: Int): String = {
    if (social == null) {
      return "No hay Registros"
    } else if (social == 1) {
      return "Alta Sociedad"
    } else if (social == 2) {
      return "Clase Media"
    } else if (social == 3) {
      return "Clase Baja"
    } else {
      return "No hay Registros"
    }

  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Titanic")
      .getOrCreate()
    //Leer el DataSet
    import spark.implicits._
    val Titanic_DF = spark.read
      .option("header", "true")
      .option("InterSchema", "true")
      .csv("C:/Users/SDS-Usuario/Downloads/spark/data/data/titanic.csv")

    Titanic_DF.show()
    Titanic_DF.printSchema()

    //Valores Sobrevivientes

    val sur = spark.udf.register("sur", (sobreviviente: Int) => {

      if (sobreviviente == null) {
        "No hay Registros"
      } else if (sobreviviente == 1) {
        "Sobrevivio"
      } else if (sobreviviente == 0) {
        "Fallecido"

      } else {
        "No hay Registros Validos"
      }
    })

    //Valores Clases Sociales
    val Social = spark.udf.register("emb", (social: Int) => {
      if (social == null) {
        "No hay Registros"
      } else if (social == 1) {
        "Alta Sociedad"
      } else if (social == 2) {
        "Clase Media"
      } else if (social == 3) {
        "Clase Baja"
      } else {
        "No hay Registros"
      }
    })

    //Personas que Sobrevivieron al Tragico

    val sobrevientes = Titanic_DF.na.drop("any", Seq("Survived"))
      .filter($"Survived" === 1)
      .withColumn("Survived", sur($"Survived"))
      .withColumn("Pclass", Social($"Pclass").as("CLASE"))
      .select(
        "Name",
        "Age",
        "Pclass",
        "Ticket",
        "Fare",
        "Cabin",
        "Embarked"
      )


    sobrevientes.createOrReplaceTempView("SURVIVOR")
    val tercera_edad = spark.sql("SELECT Name, Age, Pclass, Ticket, Fare, Cabin, Embarked FROM SURVIVOR WHERE Age>=60 order by Name")


    val embarque_C = sobrevientes.na.drop("any", Seq("Embarked"))
      .filter($"Embarked" === 'C')

      .select(
        "Name",
        "Age",
        "Pclass",
        "Ticket",
        "Fare",
        "Cabin",
        "Embarked"
      )


    //Totales
    val total_supervivientes_Titanic = sobrevientes.count()
    val total_personas_tercera_edad = tercera_edad.count()
    val total_personas_C = embarque_C.count()

    println("Total de Personas que sobrevivieron al Impacto: " + total_supervivientes_Titanic)
    println("Total de personas de la tercera Edad que sobrevivieron: " + total_personas_tercera_edad)
    println("Total de Personas que pertenecen al embarque C: " + total_personas_C)

    tercera_edad.show(10)
    sobrevientes.show(10)
    embarque_C.show(false)


    spark.stop()

  }


}
