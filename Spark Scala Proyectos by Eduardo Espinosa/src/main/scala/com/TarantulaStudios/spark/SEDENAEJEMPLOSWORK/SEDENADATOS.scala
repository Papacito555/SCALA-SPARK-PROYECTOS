package com.TarantulaStudios.spark.SEDENAEJEMPLOSWORK

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j._

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util.Calendar

object SEDENADATOS {
  def plat(plataforma:Int):String= {
    if (plataforma == null) {
      return "sin Plataforma"
    }else if( plataforma == 1){
      return "csv"
    }else if(plataforma == 2){
      return "ArcGis"
    }else if(plataforma ==3){
      return "xls"
    }else if(plataforma == 4){
      return "application/vnd.ms-excel"
    }else if(plataforma == 5){
      return "xlsx"
    }else if(plataforma == 6){
      return "word"
    }else if(plataforma == 7){
      return "PDF"
    }else if(plataforma == 8){
      return "Excel"
    } else{
      return "sin Plataforma"
    }

  }


  def main(args:Array[String])= {
    Logger.getLogger ("org").setLevel (Level.ERROR)

    val spark= SparkSession
      .builder ()
      .appName ("SEDENA_Cases")
      .master ("local[*]")
      .getOrCreate ()

    import spark.sqlContext.implicits._

    val SEDENA_df=spark.read
      .option("header","True")
      .option("inferSchema","True")
      .csv("C:/Users/SDS-Usuario/Downloads/spark/data/data/SEDENA.csv")
    val dateFormatter = new SimpleDateFormat("dd/MM/yyyy")
    SEDENA_df.printSchema()

    val plat= spark.udf.register ("plat", (plataforma: Int) => {
      if (plataforma == null) {
        "sin Plataforma"
      }else if( plataforma == 1){
        "csv"
      }else if(plataforma == 2){
        "ArcGis"
      }else if(plataforma ==3){
        "xls"
      }else if(plataforma == 4){
        "application/vnd.ms-excel"
      }else if(plataforma == 5){
        "xlsx"
      }else if(plataforma == 6){
        "word"
      }else if(plataforma == 7){
        "PDF"
      }else if(plataforma == 8){
        "Excel"
      } else{
        "sin Plataforma"
      }

    } )

    val conjunto_datos=SEDENA_df.na.drop("any",Seq("Seguridad_Datos"))
      .filter((($"Seguridad_Datos" === 1) ||
        ($"Seguridad_Datos" === 2))
      )
      .withColumn("Plataforma",plat($"Plataforma"))
      .select("Responsable",
        "Conjunto",
        "Recurso",
        "Area"
      )
    val frec_actua= SEDENA_df.na.drop("any", Seq("Fecha_Pub"))
      .filter($"Fecha_Pub" =!= "9999-99-99")
      .withColumn("Plataforma",plat($"Plataforma"))
      .select($"Conjunto",
        $"Area",
        date_format(to_date($"Fecha_Pub","yyyy-MM-dd"),"dd-MM-yyyy").as("FECHA_PUBLICACIÓN"),
        $"Frecuencia_Actualizacion",
        lit(dateFormatter.format(Calendar.getInstance().getTime())).as("FECHA_ACTUAL"))

    SEDENA_df.createOrReplaceTempView("DATOS")

    val excel = spark.sql("SELECT Responsable, Plataforma, Conjunto FROM DATOS where Plataforma== 1 OR Plataforma ==3 OR Plataforma ==5 OR Plataforma ==8 order by Responsable")
      .withColumn("Plataforma",plat($"Plataforma"))
    excel.show()

    val total_records_analized=SEDENA_df.count()
    val total_records_frec_actua=frec_actua.count()
    val total_archivos_excel = excel.count()

    println("Total de caso analizados = "+total_records_analized)
    println("Total de frecuencia actualizados= "+total_records_frec_actua)
    println("Total de Archivos que pueden abrirse en Excel: " + total_archivos_excel)

    conjunto_datos.show(10)
    spark.stop()
  }

}