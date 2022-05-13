package com.TarantulaStudios.spark.Covid

import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat

object CasosCovidAbril2022 {
  //Sexo
  def sex(sexo: Int): String = {
    if (sexo == null) {
      return "No Binario"
    } else if (sexo == 1) {
      return "Hombre"
    } else if (sexo == 2) {
      return "Mujer"
    } else if (sexo == 99) {
      return "No Especificado"
    }
    else {
      return "Sin Registros"
    }
  }

  //Tipo Paciente
  def pac(paciente: Int): String = {
    if (paciente == null) {
      return "Sin Registros"
    } else if (paciente == 1) {
      return "Ambulatorio"
    } else if (paciente == 2) {
      return "Hospitalizado"
    } else if (paciente == 99) {
      return "No Especificado"
    } else {
      return "Sin Registros"
    }
  }

  //Resultados Laboratorio
  def lab(laboratorio: Int): String = {
    if (laboratorio == null) {
      return "Sin Registros"
    } else if (laboratorio == 1) {
      return "POSITIVO A SARS-COV-2"
    } else if (laboratorio == 2) {
      return "NO POSITIVO A SARS-COV-2"
    } else if (laboratorio == 3) {
      return "RESULTADO PENDIENTE"
    } else if (laboratorio == 4) {
      return "RESULTADO NO ADECUADO "
    } else if (laboratorio == 97) {
      return "NO APLICA (CASO SIN MUESTRA)"
    } else {
      return "Sin Registros"
    }
  }

  //Nacionalidad
  def nac(nacionalidad: Int): String = {
    if (nacionalidad == null) {
      return "Sin Registros"
    } else if (nacionalidad == 1) {
      return "Mexicana"
    } else if (nacionalidad == 2) {
      return "Extranjero"
    } else {
      return "Sin Registros"
    }
  }

  //Sector al que pertenece
  def sec(sector: Int): String = {
    if (sector == null) {
      return "Sin Registro"
    } else if (sector == 1) {
      return "CRUZ ROJA"
    } else if (sector == 2) {
      return "DIF"
    } else if (sector == 3) {
      return "ESTATAL"
    } else if (sector == 4) {
      return "IMSS"
    } else if (sector == 5) {
      return "IMSS-BIENESTAR"
    } else if (sector == 6) {
      return "ISSSTE"
    } else if (sector == 7) {
      return "MUNICIPAL"
    } else if (sector == 8) {
      return "PEMEX"
    } else if (sector == 9) {
      return "PRIVADA"
    } else if (sector == 10) {
      return "SEDENA"
    } else if (sector == 11) {
      return "SEMAR"
    } else if (sector == 13) {
      return "SSA"
    } else if (sector == 14) {
      return "UNIVERSITARIO"
    } else if (sector == 15) {
      return "NO ESPECIFICADO"
    } else {
      return "Sin Registros"
    }
  }

  //Origen
  def org(origen: Int): String = {
    if (origen == null) {
      return "Sin Registros"
    } else if (origen == 1) {
      return "USMER"
    } else if (origen == 2) {
      return "FUERA DE USMER"
    } else if (origen == 99) {
      return "NO ESPECIFICADO"
    } else {
      return "Sin Registros"
    }
  }

  //Resultado Antigeno
  def antg(antigeno: Int): String = {
    if (antigeno == null) {
      return "Sin Registros"
    } else if (antigeno == 1) {
      return "POSITIVO A SARS-COV-2"
    } else if (antigeno == 2) {
      return "NEGATIVO A SARS-COV-2"
    } else if (antigeno == 97) {
      return "NO APLICA (CASO SIN MUESTRA)"
    } else {
      return "Sin Registros"
    }
  }

  def emb(embarazo: Int): String = {
    if (embarazo == null) {
      return "No hay Registros"
    } else if (embarazo == 1) {
      return "Embarazada"
    } else if (embarazo == 2) {
      return "No Embarazada"
    } else if (embarazo == 97) {
      return "No Aplica"
    } else if (embarazo == 98) {
      return "Se ignora"
    } else {
      return "No hay Registros"
    }
  }

  def main(args: Array[String]) = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Casos Covid a Abril del 2022")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val covid = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:/Users/SDS-Usuario/Downloads/spark/data/data/covid19/220404COVID19MEXICO.csv")

    val dateFormatter = new SimpleDateFormat("dd/MM/yyyy")

    covid.printSchema()

    //Sexooooooo ( ͡° ͜ʖ ͡°)
    val sex = spark.udf.register("sex", (sexo: Int) => {
      if (sexo == null) {
        "No Binario"
      } else if (sexo == 1) {
        "Hombre"
      } else if (sexo == 2) {
        "Mujer"
      } else if (sexo == 99) {
        "No Especificado"
      }
      else {
        "Sin Registros"
      }
    })

    //Pacientes (⊃｡•́‿•̀｡)⊃
    val pac = spark.udf.register("pac", (paciente: Int) => {
      if (paciente == null) {
        "Sin Registros"
      } else if (paciente == 1) {
        "Ambulatorio"
      } else if (paciente == 2) {
        "Hospitalizado"
      } else if (paciente == 99) {
        "No Especificado"
      } else {
        "Sin Registros"
      }
    })

    //Resultados Laboratorio (｡◕‿◕｡)
    val lab = spark.udf.register("lab", (laboratorio: Int) => {
      if (laboratorio == null) {
        "Sin Registros"
      } else if (laboratorio == 1) {
        "POSITIVO A SARS-COV-2"
      } else if (laboratorio == 2) {
        "NO POSITIVO A SARS-COV-2"
      } else if (laboratorio == 3) {
        "RESULTADO PENDIENTE"
      } else if (laboratorio == 4) {
        "RESULTADO NO ADECUADO "
      } else if (laboratorio == 97) {
        "NO APLICA (CASO SIN MUESTRA)"
      } else {
        "Sin Registros"
      }
    })

    //Nacionalidad  ʕ•́ᴥ•̀ʔっ
    val nac = spark.udf.register("nac", (nacionalidad: Int) => {
      if (nacionalidad == null) {
        "Sin Registros"
      } else if (nacionalidad == 1) {
        "Mexicana"
      } else if (nacionalidad == 2) {
        "Extranjero"
      } else {
        "Sin Registros"
      }
    })

    //Sectores ( ͡° ͜ʖ ͡°)
    val sec = spark.udf.register("sec", (sector: Int) => {
      if (sector == null) {
        "Sin Registro"
      } else if (sector == 1) {
        "CRUZ ROJA"
      } else if (sector == 2) {
        "DIF"
      } else if (sector == 3) {
        "ESTATAL"
      } else if (sector == 4) {
        "IMSS"
      } else if (sector == 5) {
        "IMSS-BIENESTAR"
      } else if (sector == 6) {
        "ISSSTE"
      } else if (sector == 7) {
        "MUNICIPAL"
      } else if (sector == 8) {
        "PEMEX"
      } else if (sector == 9) {
        "PRIVADA"
      } else if (sector == 10) {
        "SEDENA"
      } else if (sector == 11) {
        "SEMAR"
      } else if (sector == 13) {
        "SSA"
      } else if (sector == 14) {
        "UNIVERSITARIO"
      } else if (sector == 15) {
        "NO ESPECIFICADO"
      } else {
        "Sin Registros"
      }
    })

    //Origen  ( ͡❛ ͜ʖ ͡❛)✊
    val org = spark.udf.register("org", (origen: Int) => {
      if (origen == null) {
        "Sin Registros"
      } else if (origen == 1) {
        "USMER"
      } else if (origen == 2) {
        "FUERA DE USMER"
      } else if (origen == 99) {
        "NO ESPECIFICADO"
      } else {
        "Sin Registros"
      }
    })

    //Antigeno   (っ ͡❛ ͜ʖ ͡❛)っ🎔
    val antg = spark.udf.register("antg", (antigeno: Int) => {
      if (antigeno == null) {
        "Sin Registros"
      } else if (antigeno == 1) {
        "POSITIVO A SARS-COV-2"
      } else if (antigeno == 2) {
        "NEGATIVO A SARS-COV-2"
      } else if (antigeno == 97) {
        "NO APLICA (CASO SIN MUESTRA)"
      } else {
        "Sin Registros"
      }
    })

    //Valores Embarazo  ≧◠ᴥ◠≦✊
    val emb = spark.udf.register("emb", (embarazo: Int) => {
      if (embarazo == null) {
        "No hay Registros"
      } else if (embarazo == 1) {
        "Embarazada"
      } else if (embarazo == 2) {
        "No Embarazada"
      } else if (embarazo == 97) {
        "No Aplica"
      } else if (embarazo == 98) {
        "Se ignora"
      } else {
        "No hay Registros"
      }
    })

    val casos_positivos = covid.na.drop("any", Seq("CLASIFICACION_FINAL"))
      .filter((($"CLASIFICACION_FINAL" === 1) ||
        ($"CLASIFICACION_FINAL" === 2) ||
        ($"CLASIFICACION_FINAL" === 3)))
      .withColumn("SEXO", sex($"SEXO"))
      .withColumn("TIPO_PACIENTE", pac($"TIPO_PACIENTE"))
      .withColumn("NACIONALIDAD", nac($"NACIONALIDAD"))
      .withColumn("RESULTADO_LAB", lab($"RESULTADO_LAB"))
      .withColumn("SECTOR", sec($"SECTOR"))
      .withColumn("ORIGEN", org($"ORIGEN"))
      .withColumn("RESULTADO_ANTIGENO", antg($"RESULTADO_ANTIGENO"))
      .withColumn("EMBARAZO", emb($"EMBARAZO"))
      .select("ID_REGISTRO",
        "CLASIFICACION_FINAL",
        "SEXO",
        "TIPO_PACIENTE",
        "NACIONALIDAD",
        "RESULTADO_LAB",
        "SECTOR",
        "ORIGEN",
        "RESULTADO_ANTIGENO",
        "EMBARAZO"
      )


    val SEDENA_covid = casos_positivos.na.drop("any", Seq("SECTOR"))
      .filter($"SECTOR" === "SEDENA")
      .select("ID_REGISTRO",
        "CLASIFICACION_FINAL",
        "SEXO",
        "TIPO_PACIENTE",
        "NACIONALIDAD",
        "RESULTADO_LAB",
        "SECTOR",
        "ORIGEN",
        "RESULTADO_ANTIGENO")


    val Fuerzas_Armadas_Covid = casos_positivos.na.drop("any", Seq("SECTOR"))
      .filter((($"SECTOR" === "SEDENA") ||
        ($"SECTOR" === "SEMAR")))
      .select("ID_REGISTRO",
        "CLASIFICACION_FINAL",
        "SEXO",
        "TIPO_PACIENTE",
        "NACIONALIDAD",
        "RESULTADO_LAB",
        "SECTOR",
        "ORIGEN",
        "RESULTADO_ANTIGENO")
    casos_positivos.createOrReplaceTempView("POSITIVOS")

    // val SEDENA = spark.sql("SELECT ID_REGISTRO, CLASIFICACION_FINAL, SEXO, TIPO_PACIENTE, NACIONALIDAD, RESULTADO_LAB, SECTOR, ORIGEN, RESULTADO_ANTIGENO FROM POSITIVOS where SECTOR == 10")


    val total_casos_positivos = casos_positivos.count()
    val total_positivos_SEDENA = SEDENA_covid.count()
    val total_fuerzas_armadas = Fuerzas_Armadas_Covid.count()

    println("AHI ESTA MI GATO MIS CHAVITOS!!!!!!")
    println("░░░░░░░░░░░░░░░░░░░░░▄▀░░▌")
    println("░░░░░░░░░░░░░░░░░░░▄▀▐░░░▌")
    println("░░░░░░░░░░░░░░░░▄▀▀▒▐▒░░░▌")
    println("░░░░░▄▀▀▄░░░▄▄▀▀▒▒▒▒▌▒▒░░▌")
    println("░░░░▐▒░░░▀▄▀▒▒▒▒▒▒▒▒▒▒▒▒▒█ ")
    println("░░░░▌▒░░░░▒▀▄▒▒▒▒▒▒▒▒▒▒▒▒▒▀▄ ")
    println("░░░░▐▒░░░░░▒▒▒▒▒▒▒▒▒▌▒▐▒▒▒▒▒▀▄ ")
    println("░░░░▌▀▄░░▒▒▒▒▒▒▒▒▐▒▒▒▌▒▌▒▄▄▒▒▐ ")
    println("░░░▌▌▒▒▀▒▒▒▒▒▒▒▒▒▒▐▒▒▒▒▒█▄█▌▒▒▌ ")
    println("░▄▀▒▐▒▒▒▒▒▒▒▒▒▒▒▄▀█▌▒▒▒▒▒▀▀▒▒▐░░░▄ ")
    println("▀▒▒▒▒▌▒▒▒▒▒▒▒▄▒▐███▌▄▒▒▒▒▒▒▒▄▀▀▀▀")
    println("▒▒▒▒▒▐▒▒▒▒▒▄▀▒▒▒▀▀▀▒▒▒▒▄█▀░░▒▌▀▀▄▄ ")
    println("▒▒▒▒▒▒█▒▄▄▀▒▒▒▒▒▒▒▒▒▒▒░░▐▒▀▄▀▄░░░░▀ ")
    println("▒▒▒▒▒▒▒█▒▒▒▒▒▒▒▒▒▄▒▒▒▒▄▀▒▒▒▌░░▀▄ ")
    println("▒▒▒▒▒▒▒▒▀▄▒▒▒▒▒▒▒▒▀▀▀▀▒▒▒▄▀  ")


    casos_positivos.show()
    SEDENA_covid.show()
    Fuerzas_Armadas_Covid.show()

    println("Total de Casos Positivos a Covid 19 a Abril de 2022: " + total_casos_positivos)
    println("Total de Casos Positivos dentro de la SEDENA: " + total_positivos_SEDENA)
    println("Total de Casos Positivos dentro de las Fuerzas Armadas Mexicanas: " + total_fuerzas_armadas)
    /*
    for (i <- 0 to 4)
    print (i)
    println ()
    for (i1 <- 0 to 3)
    print(i1 + 1)
    println()
    for (i2 <- 1 to 8 if i2 < 5)
    print(i2)
    println()
    for (i3 <- 1 to 4) print(i3)
*/

  }


}
