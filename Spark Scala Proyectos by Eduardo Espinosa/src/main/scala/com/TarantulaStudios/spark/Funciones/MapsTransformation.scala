package com.TarantulaStudios.spark.Funciones

import com.TarantulaStudios.spark.Clases.Util
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object MapsTransformation {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Transformacion de Mapas")
      .getOrCreate()

    val structureData = Seq(
      Row("GARA630309HDFRZN07", "Antonio", "", "Garcia", "Ruiz", 58, "M", "Militar Activo", "antoniogarciaruiz@me.com", 100000),
      Row("RASA571221HGRDSG03", "Agustin", "", "Radilla", "Suastegui", 63, "M", "Militar Activo", "subsecretariodefnac@hotmail.com", 150000),
      Row("PIBA770106HTLDZN06", "Antonio", "", "Piedras", "Baez", 45, "M", "Militar Activo", "antoniopiedras@sedena.gob.mx", 35000),
      Row("MAGA890327MMCRRR05", "Ariana", "", "Martinez", "Garcia", 32, "F", "Militar Activo", "caboariana5678@outlook.com", 12000),
      Row("BAMA700819HDFZRR00", "Armando", "", "Bazan", "Martinez", 51, "M", "Militar Activo", "armandovincentbazan@gmail.com", 20000),
      Row("RELA600816HMSSNR01", "Arturo", "", "Resendiz", "Leana", 61, "M", "Militar Retirado", "arturoresendiz@outlook.es", 100000),
      Row("PAEC580106HVZNSR08", "Carlos", "Arturo", "Pancardo", "Escudero", 64, "M", "Militar Activo", "leinspectorfuerzaaerea@sedena.gob.mx", 130000),
      Row("AASC960420MMCLLR03", "Carolina", "", "Alvarado", "Solis", 25, "F", "Militar Activo", "carolinaalvaradoinfo@yopmail.com", 10000),
      Row("OOCC990518MDFCVZ00", "Cazandra", "Guadalupe", "Ochoa", "Cuevas", 22, "F", "Militar Activo", "cazandragdlpe345@gmail.com", 10000),
      Row("SACC780717MTSNRN08", "Concepcion", "", "Sanchez", "Cruz", 43, "F", "Derechohabiente", "concepcion37893@hotmail.com", -1),
      Row("HEMD950317HDFRTV09", "David", "", "Hernandez", "Mota", 26, "M", "Militar Activo", "davidmotx@gmail.com", 16000),
      Row("TEJD001120HCHRRVA0", "David", "Raymundo", "Trejo", "Juarez", 20, "M", "Derechohabiente", "davidtrejo3456789@hotmail.com", -1),
      Row("BARE880703HHGTZD00", "Eden", "", "Bautista", "Ruiz", 33, "M", "Militar Activo", "edenbautistasgto@gmail.com", 16000),
      Row("OIRE921124HDFRCD01", "Edgar", "Alan", "Ortiz", "Rocha", 29, "M", "Militar Activo", "alanrocha12234@gmail.com", 10000),
      Row("MOME600104HDGNRD07", "Edgardo", "Alonso", "Montelongo", "Mercado", 62, "M", "Militar Activo", "graledgardomontelongo12@outlook.com", 100000),
      Row("EIME980802HPLSND04", "Eduardo", "Angel", "Espinosa", "Montalvo", 23, "M", "Jefe Poder Ejecutivo", "jancintomotes1234@gmail.com", 5000000),
      Row("BAME691013HOCRRD06", "Eduardo", "Teofilo", "Barrios", "Morales", 52, "M", "Militar Retirado", "eduardoteofilosgto1ro@outlook.com", 18000),
      Row("AUAE950326HMCGGL06", "Elias", "", "Aguilar", "Aguilar", 26, "M", "Militar Activo", "sldeliasaguilara@outlook.com.es", 10000),
      Row("JIRE850801HTSMBR02", "Ernesto", "Alfonso", "Jimenez", "Robles", 36, "M", "Militar Activo", "alfonsotech12@gmail.com", 18000),
      Row("GARG601222HPLRNB09", "Gabriel", "", "Garcia", "Rincon", 60, "M", "Militar Activo", "oficialmayorsedena@sedena.gob.mx", 140000),
      Row("AORG720204HGRNSL05", "Gilberto", "", "Antonio", "Resendiz", 50, "M", "Militar Activo", "garesendiz@gmail.com", 50000),
      Row("MATG591212HTLZRD02", "Guadalupe", "", "Maza", "De la Torre", 61, "M", "Militar Activo", "gralmazadelatorre@gmail.com", 120000),
      Row("GARG970819MHGRJD02", "Guadalupe", "Juana", "Garcia", "Rojas", 24, "F", "Militar Activo", "lupipisgarcia234@yahoo.com", 10000),
      Row("NOVH630312HDFYLC08", "Hector", "Faustino", "Noyola", "Villalobos", 58, "M", "Militar Activo", "hectornoyolavilla@yahoo.com.mx", 100000),
      Row("TUZJ811027HMCRMQ01", "Joaquin", "Armando", "Trujillo", "Zamudio", 40, "M", "Militar Activo", "joaquintrujillo@me.com", 50000),
      Row("LEZJ670914HGTNRR04", "Jorge", "Aurelio", "Leon", "Zarate", 54, "M", "Militar Activo", "coronelLion@hotmail.com", 80000),
      Row("LOPJ850821HDFPNR09", "Jorge", "Luis", "Lopez", "Pineda", 36, "M", "Militar Activo", "cabopinedaeloriginal@hotmail.com", 12000),
      Row("VERG561221HSLGVR00", "Jose", "Gerardo", "Vega", "Rivera", 65, "M", "Militar Activo", "cmdntefuerzaaereamexicana@sedena.gob.mx", 150000),
      Row("JIAL690227HMCMCN02", "Jose", "Leandro", "Jimenez", "Acosta", 52, "M", "Militar Retirado", "joseleandrojeque@hotmail.com", 20000),
      Row("HEAL881210HMSRLS06", "Jose", "Luis", "Hernandez", "Alvarez", 33, "M", "Militar Activo", "joseluishernandez@yahoo.com.mx", 20000),
      Row("TOFJ870908HMCRRN03", "Juan", "Diego", "Torres", "Ferrer", 34, "M", "Militar Activo", "juantorresferrer1987@yahoo.com", 35000),
      Row("GOAJ730821HCLNRN06", "Juan", "Francisco", "Gonzalez", "Araiza", 48, "M", "Militar Activo", "sgtoaraizatecnologias@hotmail.com", 18000),
      Row("CURL750926HVZRMR00", "Lorenzo", "", "Cruz", "Ramos", 46, "M", "Militar Activo", "lenchiskanelpro@gmail.com", 18000),
      Row("SAGL600207HBCNNS09", "Luis", "Cresencio", "Sandoval", "Gonzalez", 62, "M", "Militar Activo", "atn_ciudadana@sedena.gob.mx", 200000),
      Row("RETO800313HDFYRS00", "Oscar", "", "Reynoso", "Trejo", 41, "M", "Militar Activo", "oscartrejocomisiones@hotmail.com", 12000),
      Row("BABR970908HDFLLL09", "Raul", "Adrian", "Baltazar", "Blancas", 24, "M", "Militar Activo", "sldrulas@hotmail.com", 10000),
      Row("EACR940718HDFSRB02", "Ruben", "Mauricio", "Esparza", "Carrillo", 27, "M", "Militar Activo", "soldadoesparza123@gmail.com", 10000),
      Row("CIZS480614HDFNPL06", "Salvador", "", "Cienfuegos", "Zepeda", 73, "M", "Militar Retirado", "salvadorcienfuegos@hotmail.com", 190000),
      Row("CUFS861130MDFRSL03", "Silvia", "", "De la Cruz", "Faustino", 35, "F", "Militar Activo", "chiborras123@gmail.com", 12000),
      Row("EAAJ780327MMCSBN05", "Juana", "Ofelia", "Escamilla", "Abrego", 43, "M", "Militar Activo", "ofeliaescamilla456@outlook.com", 16000),
      Row("PALJ750626MDFLGL09", "Julieta", "", "Palacios", "Lugo", 46, "M", "Militar Retirado", "cabojulieta12@hotmail.com", 12000),
      Row("EIMO041220MDFSNRA1", "Orlee", "Adina", "Espinosa", "Montalvo", 17, "F", "Derechohabiente", "orlee2preciosa@gmail.com", -1)
    )

    val structureSchema = new StructType()
      .add("CURP", StringType)
      .add("Primer_Nombre", StringType)
      .add("Segundo_Nombre", StringType)
      .add("AP", StringType)
      .add("AM", StringType)
      .add("Edad", IntegerType)
      .add("Sexo", StringType)
      .add("Estatus", StringType)
      .add("Email", StringType)
      .add("Sueldo", IntegerType)


    val df2 = spark.createDataFrame(
      spark.sparkContext.parallelize(structureData), structureSchema)
    df2.printSchema()
    df2.show(false)


    import spark.implicits._

    //Primer Manera donde no se necesita una clase externa
    val df1 = df2.map(row => {
      // val util = new Util()
      val Nombre_Completo = row.getString(1) + " " + row.getString(2) + " " + row.getString(3) + " " + row.getString(4)
      (row.getString(0), Nombre_Completo, row.getInt(5), row.getString(6), row.getString(7), row.getString(8), row.getInt(9))
    })
    val df1Map = df1.toDF("CURP", "Nombre_Completo", "Edad", "Sexo", "Estatus", "Email", "Sueldo")

    df1Map.printSchema()
    df1Map.show(false)


    //Segunda Manera de hacer un Mapa usando una Clase Externa
    val util = new Util()
    val df4 = df2.map(row => {

      val fullName = util.combine(row.getString(1), row.getString(2), row.getString(3), row.getString(4))
      (row.getString(0), fullName, row.getInt(5), row.getString(6), row.getString(7), row.getString(8), row.getInt(9))
    })
    val df4Map = df4.toDF("CURP", "fullName", "Edad", "Sexo", "Estatus", "Email", "Sueldo")

    df4Map.printSchema()
    df4Map.show(false)

    val df5 = df2.mapPartitions(iterator => {
      val util = new Util()
      val res = iterator.map(row => {
        val fullName = util.combine(row.getString(1), row.getString(2), row.getString(3), row.getString(4))
        (row.getString(0), fullName, row.getInt(5), row.getString(6), row.getString(7), row.getString(8), row.getInt(9))
      })
      res
    })
    val df5part = df5.toDF("CURP", "fullName", "Edad", "Sexo", "Estatus", "Email", "Sueldo")
    df5part.printSchema()
    df5part.show(false)


    /*Creacion archivo csv
    import spark.sqlContext.implicits._

    df2
      //.sort($"AP",$"AM",$"Primer_Nombre")
      .repartition(1).write
      .option("header","true")
      .mode(SaveMode.Overwrite)
      .csv("C:/Users/SDS-Usuario/Downloads/spark/data/data/HCM")

    Consultas SQL
    df2.createOrReplaceTempView("PACIENTES")
    val activos =spark.sql("SELECT Primer_Nombre, Segundo_Nombre, AP, AM, Edad, Sexo FROM PACIENTES  WHERE Estatus == 'Militar Activo' ORDER BY AP, AM, Primer_Nombre")
    activos.show()

    val sueldos = spark.sql("SELECT  COUNT(Sueldo), Primer_Nombre, Estatus where Estatus == 'Militar Activo' order by Sueldo")
    sueldos.show()

*/


  }

}
