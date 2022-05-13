package com.TarantulaStudios.spark.Funciones

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object MapTransformation {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Transformacion de Mapas")
      .getOrCreate()

    //Datos a Analizar
    val structure = Seq(
      Row("GARA630309HDFRZN07", "Antonio", "", "Garcia", "Ruiz", 58, "M", "B2233190", "GARUIAO090363", "Militar Activo", "antoniogarciaruiz@me.com", 100000),
      Row("RASA571221HGRDSG03", "Agustin", "", "Radilla", "Suastegui", 63, "M", "9248093", "RADUAAN211257", "Militar Activo", "subsecretariodefnac@hotmail.com", 150000),
      Row("PIBA770106HTLDZN06", "Antonio", "", "Piedras", "Baez", 45, "M", "C1253472", "PIDAEAO060177", "Militar Activo", "antoniopiedras@sedena.gob.mx", 35000),
      Row("MAGA890327MMCRRR05", "Ariana", "", "Martinez", "Garcia", 32, "F", "A10038315", "MARAIAA270389", "Militar Activo", "caboariana5678@outlook.com", 12000),
      Row("BAMA700819HDFZRR00", "Armando", "", "Bazan", "Martinez", 51, "M", "D456789", "BAMARM190870", "Militar Activo", "armandovincentbazan@gmail.com", 20000),
      Row("RELA600816HMSSNR01", "Arturo", "", "Resendiz", "Leana", 61, "M", "10543987", "RESEAAO160860", "Militar Retirado", "arturoresendiz@outlook.es", 100000),
      Row("PAEC580106HVZNSR08", "Carlos", "Arturo", "Pancardo", "Escudero", 63, "M", "9728683", "PANEUCO060158", "Militar Activo", "leinspectorfuerzaaerea@sedena.gob.mx", 130000),
      Row("AASC960420MMCLLR03", "Carolina", "", "Alvarado", "Solis", 25, "F", "A10052544", "LAVOICA200496", "Militar Activo", "carolinaalvaradoinfo@yopmail.com", 10000),
      Row("OOCC990518MDFCVZ00", "Cazandra", "Guadalupe", "Ochoa", "Cuevas", 22, "F", "A10052392", "COHUECE180599", "Militar Activo", "cazandragdlpe345@gmail.com", 10000),
      Row("SACC780717MTSNRN08", "Concepcion", "", "Sanchez", "Cruz", 43, "F", "B9662021", "SANUXCN170778", "Derechohabiente", "concepcion37893@hotmail.com", -1),
      Row("HEMD950317HDFRTV09", "David", "", "Hernandez", "Mota", 26, "M", "D3270814", "HEROADD011017", "Militar Activo", "davidmotx@gmail.com", 16000),
      Row("TEJD001120HCHRRVA0", "David", "Raymundo", "Trejo", "Juarez", 20, "M", "C286123", "TXRUADO201100", "Derechohabiente", "davidtrejo3456789@hotmail.com", -1),
      Row("BARE880703HHGTZD00", "Eden", "", "Bautista", "Ruiz", 33, "M", "C8975993", "BATUIEN030788", "Militar Activo", "edenbautistasgto@gmail.com", 16000),
      Row("OIRE921124HDFRCD01", "Edgar", "Alan", "Ortiz", "Rocha", 29, "M", "D1778385", "ROTOAEN241192", "Militar Activo", "alanrocha12234@gmail.com", 10000),
      Row("MOME600104HDGNRD07", "Edgardo", "Alonso", "Montelongo", "Mercado", 61, "M", "B190873", "MONEAEO040160", "Militar Activo", "graledgardomontelongo12@outlook.com", 100000),
      Row("EIME980802HPLSND04", "Eduardo", "Angel", "Espinosa", "Montalvo", 23, "M", "B0208123", "ESMONA020898", "Jefe Poder Ejecutivo", "jancintomotes1234@gmail.com", 5000000),
      Row("BAME691013HOCRRD06", "Eduardo", "Teofilo", "Barrios", "Morales", 52, "M", "B7993477", "BAROAEO131069", "Militar Retirado", "eduardoteofilosgto1ro@outlook.com", 18000),
      Row("AUAE950326HMCGGL06", "Elias", "", "Aguilar", "Aguilar", 26, "M", "D3063499", "GALAUES260395", "Militar Activo", "sldeliasaguilara@outlook.com.es", 10000),
      Row("JIRE850801HTSMBR02", "Ernesto", "Alfonso", "Jimenez", "Robles", 36, "M", "C7297546", "JIMOEEO010885", "Militar Activo", "alfonsotech12@gmail.com", 18000),
      Row("GARG601222HPLRNB09", "Gabriel", "", "Garcia", "Rincon", 60, "M", "10561647", "GARIOGL221260", "Militar Activo", "oficialmayorsedena@sedena.gob.mx", 140000),
      Row("AORG720204HGRNSL05", "Gilberto", "", "Antonio", "Resendiz", 50, "M", "B8542405", "NATEEGO040272", "Militar Activo", "garesendiz@gmail.com", 50000),
      Row("MATG591212HTLZRD02", "Guadalupe", "", "Maza", "De la Torre", 61, "M", "9728859", "MAZEAGE121259", "Militar Activo", "gralmazadelatorre@gmail.com", 120000),
      Row("GARG970819MHGRJD02", "Guadalupe", "Juana", "Garcia", "Rojas", 24, "F", "D1778957", "1537-167-02", "Militar Activo", "lupipisgarcia234@yahoo.com", 10000),
      Row("NOVH630312HDFYLC08", "Hector", "Faustino", "Noyola", "Villalobos", 58, "M", "B3678949", "NOYIAHO120363", "Militar Activo", "hectornoyolavilla@yahoo.com.mx", 100000),
      Row("TUZJ811027HMCRMQ01", "Joaquin", "Armando", "Trujillo", "Zamudio", 40, "M", "C6254486", "TXRAUJO271081", "Militar Activo", "joaquintrujillo@me.com", 50000),
      Row("LEZJ670914HGTNRR04", "Jorge", "Aurelio", "Leon", "Zarate", 54, "M", "B5046699", "LENAAJO140967", "Militar Activo", "coronelLion@hotmail.com", 80000),
      Row("LOPJ850821HDFPNR09", "Jorge", "Luis", "Lopez", "Pineda", 36, "M", "C6965647", "LOPIEJS210885", "Militar Activo", "cabopinedaeloriginal@hotmail.com", 12000),
      Row("VERG561221HSLGVR00", "Jose", "Gerardo", "Vega", "Rivera", 65, "M", "8704826", "VEGIEJO211256", "Militar Activo", "cmdntefuerzaaereamexicana@sedena.gob.mx", 150000),
      Row("JIAL690227HMCMCN02", "Jose", "Leandro", "Jimenez", "Acosta", 52, "M", "A10026955", "JIMAOJO270269", "Militar Retirado", "joseleandrojeque@hotmail.com", 20000),
      Row("HEAL881210HMSRLS06", "Jose", "Luis", "Hernandez", "Alvarez", 33, "M", "D0099878", "HERAAJS101288", "Militar Activo", "joseluishernandez@yahoo.com.mx", 20000),
      Row("TOFJ870908HMCRRN03", "Juan", "Diego", "Torres", "Ferrer", 34, "M", "C8555353", "TOREEJO080987", "Militar Activo", "juantorresferrer1987@yahoo.com", 35000),
      Row("GOAJ730821HCLNRN06", "Juan", "Francisco", "Gonzalez", "Araiza", 48, "M", "B9331757", "GONAAJO210873", "Militar Activo", "sgtoaraizatecnologias@hotmail.com", 18000),
      Row("CURL750926HVZRMR00", "Lorenzo", "", "Cruz", "Ramos", 46, "M", "C2192016", "CXRAOLO260975", "Militar Activo", "lenchiskanelpro@gmail.com", 18000),
      Row("SAGL600207HBCNNS09", "Luis", "Cresencio", "Sandoval", "Gonzalez", 62, "M", "10528519", "SANOALO070260", "Militar Activo", "atn_ciudadana@sedena.gob.mx", 200000),
      Row("RETO800313HDFYRS00", "Oscar", "", "Reynoso", "Trejo", 41, "M", "C3725910", "REYEOOR130380", "Militar Activo", "oscartrejocomisiones@hotmail.com", 12000),
      Row("BABR970908HDFLLL09", "Raul", "Adrian", "Baltazar", "Blancas", 24, "M", "D4775872", "19-4-17BALAARL", "Militar Activo", "sldrulas@hotmail.com", 10000),
      Row("EACR940718HDFSRB02", "Ruben", "Mauricio", "Esparza", "Carrillo", 27, "M", "D2338492", "SEPAIRN180794", "Militar Activo", "soldadoesparza123@gmail.com", 10000),
      Row("CIZS480614HDFNPL06", "Salvador", "", "Cienfuegos", "Zepeda", 73, "M", "6416991", "CINEESR140648", "Militar Retirado", "salvadorcienfuegos@hotmail.com", 190000),
      Row("CUFS861130MDFRSL03", "Silvia", "", "De la Cruz", "Faustino", 35, "F", "A10037027", "DELAUSA301186", "Militar Activo", "chiborras123@gmail.com", 12000),
      Row("EAAJ780327MMCSBN05", "Juana", "Ofelia", "Escamilla", "Abrego", 43, "M", "D4106394", "SECAEJA270378", "Militar Activo", "ofeliaescamilla456@outlook.com", 16000),
      Row("PALJ750626MDFLGL09", "Julieta", "", "Palacios", "Lugo", 46, "M", "A10029150", "PALUOJA260675", "Militar Retirado", "cabojulieta12@hotmail.com", 12000),
      Row("EIMO041220MDFSNRA1", "Orlee", "Adina", "Espinosa", "Montalvo", 17, "F", "B0208123", "EIMORL201204", "Derechohabiente", "orlee2preciosa@gmail.com", -1)
    )

    val structureSchema = new StructType()
      .add("CURP", StringType)
      .add("Primer_Nombre", StringType)
      .add("Segundo_Nombre", StringType)
      .add("AP", StringType)
      .add("AM", StringType)
      .add("Edad", IntegerType)
      .add("Sexo", StringType)
      .add("Matricula", StringType)
      .add("Expediente", StringType)
      .add("Estatus", StringType)
      .add("Email", StringType)
      .add("Sueldo", IntegerType)

    //Creacion Data Frame
    val df = spark.createDataFrame(spark.sparkContext.parallelize(structure), structureSchema)
    df.printSchema()
    df.show()
    /*
    import spark.sqlContext.implicits._

    df.sort($"AP", $"AM", $"Primer_Nombre").repartition(1).write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv("C:/Users/INformatica/Documents/SCALA/SparkScala/data/HCM")

    df.createOrReplaceTempView("PACIENTES")
    val activos = spark.sql("SELECT Primer_Nombre, Segundo_Nombre, AP, AM, Edad, Sexo FROM PACIENTES  WHERE Estatus == 'Militar Activo' ORDER BY AP, AM, Primer_Nombre")
    activos.show()

    val sueldos = spark.sql("SELECT  COUNT(Sueldo), Primer_Nombre, Estatus where Estatus == 'Militar Activo' order by Sueldo")
    sueldos.show()
    import spark.implicits._
  */

    import spark.implicits._

    //Transformacion
    val df2 = df.map(row => {
      // val util = new Util()
      val Nombre_Completo = row.getString(1) + " " + row.getString(2) + " " + row.getString(3) + " " + row.getString(4)
      (row.getString(0), Nombre_Completo, row.getInt(5), row.getString(6), row.getString(7), row.getString(8), row.getString(9), row.getString(10), row.getInt(11))
    })
    val df2Map = df2.toDF("CURP", "Nombre_Completo", "Edad", "Sexo", "Matricula", "Expediente", "Estatus", "Email", "Sueldo")

    df2Map.printSchema()
    df2Map.show(false)


  }

}
