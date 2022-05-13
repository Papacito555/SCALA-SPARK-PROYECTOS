package com.TarantulaStudios.spark.SEDENAEJEMPLOSWORK


import com.sun.crypto.provider.PBEWithMD5AndTripleDESCipher
import com.sundogsoftware.spark.Funcion_Ventana.spark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter, RightOuter}

object Pacientes_SEDENA {

  def par(parentesco:Int):String= {
    if (parentesco == null) {
      return "Militar Titular"
    } else if (parentesco == 1){
      return "Esposa"
    } else if (parentesco == 2){
      return "Esposo"
    }else if (parentesco == 3){
      return "Concubina"
    }else if (parentesco == 4){
      return "Concubino"
    }else if (parentesco == 5){
      return "Hija"
    }else if (parentesco == 6){
      return "Hijo"
    } else if (parentesco == 7){
      return "Madre"
    }else if (parentesco == 8){
      return "Padre"
    }else if (parentesco == 9){
      return "Hermana"
    }else if (parentesco == 10){
      return "Hermano"
    }else if (parentesco == 11){
      return "otro"
    }else if (parentesco == 12){
      return "Sin Parentesco"
    }else if (parentesco == 13){
      return "El Greñas"
    }else{
      return "Militar Titular"
    }

  }


  def estconsu(consulta:String): String ={
    if (consulta == null){
      return "Sin Registros"
    } else if (consulta == "A") {
      return "Pendiente Arribo"
    } else if(consulta == "B"){
      return  "Arribo"
    } else if (consulta == "L"){
      return "Llamado a toma de Signos"
    } else if (consulta == "K") {
      return "En toma de Signos"
    } else if (consulta == "C"){
      return "En Espera"
    }else if (consulta == "D"){
      return  "Demorado"
    } else if (consulta == "E"){
      return "Faltista"
    } else if (consulta == "F"){
      return "Llamado"
    } else if (consulta == "G"){
      return "En Consulta"
    } else if (consulta == 'H') {
      return "Atendido"
    }else if (consulta == "I"){
      return  "Ausente"
    } else if(consulta == "J"){
      return "Prioritario"
    }else if (consulta == "M"){
      return "Segunda Llamada"
    }else if (consulta == "N"){
      return  "Tercera Llamada Comenzamos!!!!!"
    }else if (consulta == "O"){
      return "Segunda Llamada Toma de Signos"
    }else if (consulta == "X") {
      return "Ausente Definitivo"
    }else{
      "Sin Registros"
    }
  }

  def est(estatus:String):String ={
    if (estatus ==null) {
      return "Sin Registros"
    }else if (estatus == "A") {
      return "Activo"
    } else if (estatus == "I"){
      return "Inactivo"
    }else if (estatus == "E"){
      return "Expediente Repetido"
    }else {
      return "Sin Registros"
    }
  }

  def main(args:Array[String]): Unit={

    val spark =SparkSession.builder()
      .master("local[*]")
      .appName("Filtro Militares sin Derechos")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    //Tabla Matriculas
    val militar = spark.read
      .option("header","true")
      .option("Inferschema","True")
    //.csv("C:/CONFIDENCIAL")



    //Tabla Pacientes
    val derecho = spark.read
      .option("header", "true")
      .option("Inferschema", "True")
    //.csv("C:/CONFIDENCIAL")

    //Tabla Religiones
    val religion = spark.read
      .option("header", "true")
      .option("Inferschema", "True")
    //.csv("C:/CONFIDENCIAL")

    //Tabla Solicitudes
    val solicitudes = spark.read
      .option("header", "True")
      .option("Inferschema", "true")
    //.csv("C:/CONFIDENCIAL")


    //Tabla Hospitales
    val hospital = spark.read
      .option("header", "true")
      .option("inferschema","true")
    //.csv("C:/CONFIDENCIAL")

    //Tabla Hojas de Referencia
    val hoja_referencia = spark.read
      .option("header","true")
      .option("Inferschema", "true")
      //.csv("C:/CONFIDENCIAL")

    //Valores Parentezco
    val par = spark.udf.register("par", (parentesco: Int) => {

      if (parentesco == null) {
        "Militar Titular"
      } else if (parentesco == 1){
        "Esposa"
      } else if (parentesco == 2){
        "Esposo"
      }else if (parentesco == 3){
        "Concubina"
      }else if (parentesco == 4){
        "Concubino"
      }else if (parentesco == 5){
        "Hija"
      }else if (parentesco == 6){
        "Hijo"
      } else if (parentesco == 7){
        "Madre"
      }else if (parentesco == 8){
        "Padre"
      }else if (parentesco == 9){
        "Hermana"
      }else if (parentesco == 10){
        "Hermano"
      }else if (parentesco == 11){
        "otro"
      }else if (parentesco == 12){
        "Sin Parentesco"
      }else if (parentesco == 13){
        "El Greñas"
      }else{
        "Militar Titular"
      }
    })

    //Valores Estatus
    val est = spark.udf.register("est", (estatus:String) =>{
      if (estatus ==null) {
        "Sin Registros"
      }else if (estatus == "A") {
        "Activo"
      } else if (estatus == "I"){
        "Inactivo"
      }else if (estatus == "E"){
        "Expediente Repetido"
      }else {
        "Sin Registros"
      }
    })

    //Valores Estatus Consulta
    val estconsu =spark.udf.register("estconsu", (consulta:String) =>{
      if (consulta == null){
        "Sin Registros"
      } else if (consulta == "A") {
        "Pendiente Arribo"
      } else if(consulta == "B"){
        "Arribo"
      } else if (consulta == "L"){
        "Llamado a toma de Signos"
      } else if (consulta == "K") {
        "En toma de Signos"
      } else if (consulta == "C"){
        "En Espera"
      }else if (consulta == "D"){
        "Demorado"
      } else if (consulta == "E"){
        "Faltista"
      } else if (consulta == "F"){
        "Llamado"
      } else if (consulta == "G"){
        "En Consulta"
      } else if (consulta == 'H') {
        "Atendido"
      }else if (consulta == "I"){
        "Ausente"
      } else if(consulta == "J"){
        "Prioritario"
      }else if (consulta == "M"){
        "Segunda Llamada"
      }else if (consulta == "N"){
        "Tercera Llamada Comenzamos!!!!!"
      }else if (consulta == "O"){
        "Segunda Llamada Toma de Signos"
      }else if (consulta == "X") {
        "Ausente Definitivo"
      }else{
        "Sin Registros"
      }
    })


    import spark.sqlContext.implicits._
    //Templates

    /*
   militar.createOrReplaceTempView("MILITARES")
    derecho.createOrReplaceTempView("PACIENTES")
    religion.createOrReplaceTempView("RELIGION")
    solicitudes.createOrReplaceTempView("SOLICITUDES")
    hospital.createOrReplaceTempView("HOSPITALES")
    hoja_referencia.createOrReplaceTempView("REFERENCIAS")

     */

    //Consultas
    val garru = spark.sql("SELECT * FROM MILITARES WHERE matricula = 'CONFIDENCIAL'")

    garru.show()

    val leajim = spark.sql("SELECT  * FROM MILITARES WHERE matricula = 'CONFIDENCIAL' ")
    leajim.show()

    leajim.sort($"expediente").repartition(1).write
      .option("Header", "True")
      .mode(SaveMode.Overwrite)
      .csv("C:/Users/SDS-Usuario/Downloads/spark/data/data/HCM_PACIENTE MILITAR")


    val lench = spark.sql("SELECT * FROM MILITARES WHERE ematricula = 'CONFIDENCIAL'")
    lench.show()

    val doritos = spark.sql("SELECT * FROM MILITARES WHERE matricula = 'CONFIDENCIAL'")
    doritos.show()

    val secdef = spark.sql("SELECT * FROM MILITARES WHERE matricula = 'CONFIDENCIAL'")
    secdef.show()

    val neto = spark.sql("SELECT * FROM MILITARES WHERE matricula = 'CONFIDENCIAL'")
    neto.show()

    val leajim2 = spark.sql("SELECT * FROM MILITARES WHERE matricula = 'CONFIDENCIAL'")
    leajim2.show()

    val gare = spark.sql("SELECT * FROM MILITARES WHERE matricula = 'CONFIDENCIAL'")
    gare.show()

    val patrick = spark.sql("SELECT * FROM MILITARES WHERE matricula = 'CONFIDENCIAL'")
    patrick.show()

    val all_mine = spark.sql("SELECT * FROM MILITARES where matricula = 'CONFIDENCIAL'")
    all_mine.show()

    val personal_bloqueado =spark.sql("SELECT * FROM MILITARES WHERE bloqueado = 'f'")
    personal_bloqueado.show()

    val actualizaciones_2017 = spark.sql("SELECT  * FROM MILITARES WHERE actualizado like  '%2/8/2017%'")
    actualizaciones_2017.show()

    val generales_Brigada = spark.sql("SELECT * FROM MILITARES Where empleo like '%GRAL.BGDA%' order by  fechainicio ")
    generales_Brigada.show()

    val pacientes = spark.sql("SELECT P.expediente, curp,nombre, apellidopaterno, apellidomaterno, fechanacimiento, matricula, paparentesco FROM PACIENTES as P INNER JOIN MILITARES AS M ON P.expediente == M.expediente WHERE M.matricula =='CONFIDENCIAL'")
      .withColumn("paparentesco",par($"paparentesco"))

    val GarAAAA = spark.sql("SELECT P.expediente, curp,nombre, apellidopaterno, apellidomaterno, fechanacimiento, matricula, paparentesco FROM PACIENTES as P INNER JOIN MILITARES AS M ON P.expediente == M.expediente WHERE M.matricula =='CONFIDENCIAL'")
      .withColumn("paparentesco",par($"paparentesco"))


    val Lenc = spark.sql("SELECT P.expediente, curp,nombre, apellidopaterno, apellidomaterno, fechanacimiento, matricula, paparentesco FROM PACIENTES as P INNER JOIN MILITARES AS M ON P.expediente == M.expediente WHERE M.matricula =='CONFIDENCIAL'")
      .withColumn("paparentesco",par($"paparentesco"))

    val den = spark.sql("SELECT P.expediente, curp,nombre, apellidopaterno, apellidomaterno, fechanacimiento, matricula, paparentesco FROM PACIENTES as P INNER JOIN MILITARES AS M ON P.expediente == M.expediente WHERE M.matricula =='CONFIDENCIAL'")
      .withColumn("paparentesco",par($"paparentesco"))

    val neta = spark.sql("SELECT P.expediente, curp,nombre, apellidopaterno, apellidomaterno, fechanacimiento, matricula, paparentesco FROM PACIENTES as P INNER JOIN MILITARES AS M ON P.expediente == M.expediente WHERE M.matricula =='CONFIDENCIAL'")
      .withColumn("paparentesco",par($"paparentesco"))


    val karin = spark.sql("SELECT P.expediente, curp,nombre, apellidopaterno, apellidomaterno, fechanacimiento, matricula, paparentesco FROM PACIENTES as P INNER JOIN MILITARES AS M ON P.expediente == M.expediente WHERE M.matricula =='CONFIDENCIAL'")
      .withColumn("paparentesco",par($"paparentesco"))

    val riza = spark.sql("SELECT P.expediente, curp,nombre, apellidopaterno, apellidomaterno, fechanacimiento, matricula, paparentesco FROM PACIENTES as P INNER JOIN MILITARES AS M ON P.expediente == M.expediente WHERE M.matricula =='CONFIDENCIAL'")
      .withColumn("paparentesco",par($"paparentesco"))

    val ferrero = spark.sql("SELECT P.expediente, curp,nombre, apellidopaterno, apellidomaterno, fechanacimiento, matricula, paparentesco FROM PACIENTES as P INNER JOIN MILITARES AS M ON P.expediente == M.expediente WHERE M.matricula =='CONFIDENCIAL'")
      .withColumn("paparentesco",par($"paparentesco"))

    val Suki = spark.sql("SELECT P.expediente, curp,nombre, apellidopaterno, apellidomaterno, fechanacimiento, matricula, paparentesco FROM PACIENTES as P INNER JOIN MILITARES AS M ON P.expediente == M.expediente WHERE M.matricula =='CONFIDENCIAL'")
      .withColumn("paparentesco",par($"paparentesco"))

   

    val Sonic = spark.sql("SELECT P.expediente, curp,nombre, apellidopaterno, apellidomaterno, fechanacimiento, matricula, paparentesco FROM PACIENTES as P INNER JOIN MILITARES AS M ON P.expediente == M.expediente WHERE M.matricula =='CONFIDENCIAL'")
      .withColumn("paparentesco",par($"paparentesco"))
    val pato_purifick = spark.sql("SELECT P.expediente, curp,nombre, apellidopaterno, apellidomaterno, fechanacimiento, matricula, paparentesco FROM PACIENTES as P INNER JOIN MILITARES AS M ON P.expediente == M.expediente WHERE M.matricula =='CONFIDENCIAL'")
      .withColumn("paparentesco",par($"paparentesco"))
    val Pro = spark.sql("SELECT CONFIDENCIAL  FROM PACIENTES as P INNER JOIN MILITARES AS M   ON P.expediente == M.expediente  WHERE P.motivo LIKE '%CONFIDENCIAL%'")
      .withColumn("paparentescoclave",par($"paparentescoclave"))




    val solicitudes_activas = spark.sql("SELECT P.expediente, curp,nombre, apellidopaterno, apellidomaterno, fechanacimiento, matricula, clave, estatus, licitudesfechasolicitud, odosolicitudestipopaciente, solicitudesprocedenciahist, solicitudesespecialdiad, solicitudesusuariocita, sestatusconsulta, estatussolicitud, prioridad, tipocita  FROM PACIENTES as P INNER JOIN MILITARES AS M   ON P.expediente == M.expediente JOIN SOLICITUDES AS S ON P.expediente == S.expediente  WHERE S.estatusconsulta == 'A'")
      .withColumn("clave",par($"¿clave"))
      .withColumn("estatusconsulta",estconsu($"estatusconsulta"))
      .withColumn("estatus", est($"estatus"))

    val hospitales = spark.sql("SELECT * FROM HOSPITALES WHERE hospitalid ==4")
    val referencias = spark.sql("SELECT * FROM REFERENCIAS WHERE hospitalid = 4")


    //Impresiones de Tablas
    pacientes.show()

    Lenc.show()
    den.show()
    neta.show()

    karin.show()
    riza.show()
    Suki.show()
    Sonic.show()
    pato_purifick.show()
    ferrero.show()
    Pro.show()
    solicitudes_activas.show()
    hospitales.show()
    referencias.show()


    //Contadores
    //val total_pacientes = paciente.count()
    val total_actualizaciones_2017 = actualizaciones_2017.count()
    val total_generales_Brigada = generales_Brigada.count()
    val total_pacientes_bloqueados = personal_bloqueado.count()
    val total_pro = Pro.count()


    //println("Total de pacientes militares inscritos en la SEDENA: " + total_pacientes)
    println("Total de Modificaciones en Agosto del 2017: " + total_actualizaciones_2017)
    println("Total de Generales de Brigada que se encuentran en la SEDENA: " + total_generales_Brigada)
    println("Total de personas bloqueadas: "+ total_pacientes_bloqueados)
    println("Total de personas 1100111: " + total_pro)

    spark.stop()

  }
}
