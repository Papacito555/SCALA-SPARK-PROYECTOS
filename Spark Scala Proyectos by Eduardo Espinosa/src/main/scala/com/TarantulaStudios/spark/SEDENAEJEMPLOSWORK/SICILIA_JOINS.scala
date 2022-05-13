package com.TarantulaStudios.spark.SEDENAEJEMPLOSWORK

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter, RightOuter}

import org.apache.spark.sql.Row

object SICILIA_JOINS {

  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Sistema de Citas en Linea")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.sqlContext.implicits._

    val usuarios = Seq(
      ("GARA630309HDFRZN07", "Antonio", "", "Garcia", "Ruiz", 58, "M","B2233190","GARUIAO090363", "Militar Activo", "antoniogarciaruiz@me.com", 100000,1,6),
      ("RASA571221HGRDSG03", "Agustin", "", "Radilla", "Suastegui", 63, "M", "9248093","RADUAAN211257", "Militar Activo", "subsecretariodefnac@hotmail.com", 150000,2,6),
      ("PIBA770106HTLDZN06", "Antonio", "", "Piedras", "Baez", 45, "M","C1253472","PIDAEAO060177", "Militar Activo", "antoniopiedras@sedena.gob.mx", 35000,3,2),
      ("MAGA890327MMCRRR05", "Ariana", "", "Martinez", "Garcia", 32, "F","A10038315","MARAIAA270389", "Militar Activo", "caboariana5678@outlook.com", 12000,4,3),
      ("BAMA700819HDFZRR00", "Armando", "", "Bazan", "Martinez", 51, "M","D456789","BAMARM190870", "Militar Activo", "armandovincentbazan@gmail.com", 20000,4,1),
      ("RELA600816HMSSNR01", "Arturo", "", "Resendiz", "Leana", 61, "M","10543987","RESEAAO160860", "Militar Retirado", "arturoresendiz@outlook.es", 100000,5,4),
      ("PAEC580106HVZNSR08", "Carlos", "Arturo", "Pancardo", "Escudero", 63, "M","9728683","PANEUCO060158", "Militar Activo", "leinspectorfuerzaaerea@sedena.gob.mx", 130000,6,3),
      ("AASC960420MMCLLR03", "Carolina", "", "Alvarado", "Solis", 25, "F","A10052544","LAVOICA200496", "Militar Activo", "carolinaalvaradoinfo@yopmail.com", 10000,7,6),
      ("OOCC990518MDFCVZ00", "Cazandra", "Guadalupe", "Ochoa", "Cuevas", 22, "F","A10052392","COHUECE180599", "Militar Activo", "cazandragdlpe345@gmail.com", 10000,8,4),
      ("SACC780717MTSNRN08", "Concepcion", "", "Sanchez", "Cruz", 43, "F","B9662021","SANUXCN170778", "Derechohabiente", "concepcion37893@hotmail.com", -1,9,2),
      ("HEMD950317HDFRTV09", "David", "", "Hernandez", "Mota", 26, "M","D3270814","HEROADD011017", "Militar Activo", "davidmotx@gmail.com", 16000,12,3),
      ("TEJD001120HCHRRVA0", "David", "Raymundo", "Trejo", "Juarez", 20, "M","C286123","TXRUADO201100", "Derechohabiente", "davidtrejo3456789@hotmail.com", -1,4,2),
      ("BARE880703HHGTZD00", "Eden", "", "Bautista", "Ruiz", 33, "M","C8975993","BATUIEN030788", "Militar Activo", "edenbautistasgto@gmail.com", 16000,5,3),
      ("OIRE921124HDFRCD01", "Edgar", "Alan", "Ortiz", "Rocha", 29, "M","D1778385","ROTOAEN241192", "Militar Activo", "alanrocha12234@gmail.com", 10000,6,4),
      ("MOME600104HDGNRD07", "Edgardo", "Alonso", "Montelongo", "Mercado", 61, "M","B190873","MONEAEO040160", "Militar Activo", "graledgardomontelongo12@outlook.com", 100000,13,5),
      ("EIME980802HPLSND04", "Eduardo", "Angel", "Espinosa", "Montalvo", 23, "M","B0208123","ESMONA020898", "Jefe Poder Ejecutivo", "jancintomotes1234@gmail.com", 5000000,16,6),
      ("BAME691013HOCRRD06", "Eduardo", "Teofilo", "Barrios", "Morales", 52, "M","B7993477","BAROAEO131069", "Militar Retirado", "eduardoteofilosgto1ro@outlook.com", 18000,15,1),
      ("AUAE950326HMCGGL06", "Elias", "", "Aguilar", "Aguilar", 26, "M", "D3063499","GALAUES260395", "Militar Activo", "sldeliasaguilara@outlook.com.es", 10000,12,1),
      ("JIRE850801HTSMBR02", "Ernesto", "Alfonso", "Jimenez", "Robles", 36, "M", "C7297546","JIMOEEO010885", "Militar Activo", "alfonsotech12@gmail.com", 18000,2,2),
      ("GARG601222HPLRNB09", "Gabriel", "", "Garcia", "Rincon", 60, "M", "10561647","GARIOGL221260", "Militar Activo", "oficialmayorsedena@sedena.gob.mx", 140000,3,3),
      ("AORG720204HGRNSL05", "Gilberto", "", "Antonio", "Resendiz", 50, "M", "B8542405","NATEEGO040272", "Militar Activo", "garesendiz@gmail.com", 50000,10,1),
      ("MATG591212HTLZRD02", "Guadalupe", "", "Maza", "De la Torre", 61, "M", "9728859","MAZEAGE121259", "Militar Activo", "gralmazadelatorre@gmail.com", 120000,5,4),
      ("GARG970819MHGRJD02", "Guadalupe", "Juana", "Garcia", "Rojas", 24, "F", "D1778957","1537-167-02", "Militar Activo", "lupipisgarcia234@yahoo.com", 10000,5,5),
      ("NOVH630312HDFYLC08", "Hector", "Faustino", "Noyola", "Villalobos", 58, "M", "B3678949","NOYIAHO120363", "Militar Activo", "hectornoyolavilla@yahoo.com.mx", 100000,11,6),
      ("TUZJ811027HMCRMQ01", "Joaquin", "Armando", "Trujillo", "Zamudio", 40, "M" ,"C6254486","TXRAUJO271081", "Militar Activo", "joaquintrujillo@me.com", 50000,10,1),
      ("LEZJ670914HGTNRR04", "Jorge", "Aurelio", "Leon", "Zarate", 54, "M","B5046699","LENAAJO140967", "Militar Activo", "coronelLion@hotmail.com", 80000,13,6),
      ("LOPJ850821HDFPNR09", "Jorge", "Luis", "Lopez", "Pineda", 36, "M","C6965647","LOPIEJS210885", "Militar Activo", "cabopinedaeloriginal@hotmail.com", 12000,14,3),
      ("VERG561221HSLGVR00", "Jose", "Gerardo", "Vega", "Rivera", 65, "M","8704826","VEGIEJO211256", "Militar Activo", "cmdntefuerzaaereamexicana@sedena.gob.mx", 150000,15,2),
      ("JIAL690227HMCMCN02", "Jose", "Leandro", "Jimenez", "Acosta", 52, "M","A10026955","JIMAOJO270269", "Militar Retirado", "joseleandrojeque@hotmail.com", 20000,13,2),
      ("HEAL881210HMSRLS06", "Jose", "Luis", "Hernandez", "Alvarez", 33, "M","D0099878","HERAAJS101288", "Militar Activo", "joseluishernandez@yahoo.com.mx", 20000,11,2),
      ("TOFJ870908HMCRRN03", "Juan", "Diego", "Torres", "Ferrer", 34, "M","C8555353","TOREEJO080987", "Militar Activo", "juantorresferrer1987@yahoo.com", 35000,10,1),
      ("GOAJ730821HCLNRN06", "Juan", "Francisco", "Gonzalez", "Araiza", 48, "M","B9331757","GONAAJO210873", "Militar Activo", "sgtoaraizatecnologias@hotmail.com", 18000,9,4),
      ("CURL750926HVZRMR00", "Lorenzo", "", "Cruz", "Ramos", 46, "M","C2192016","CXRAOLO260975", "Militar Activo", "lenchiskanelpro@gmail.com", 18000,7,5),
      ("SAGL600207HBCNNS09", "Luis", "Cresencio", "Sandoval", "Gonzalez", 62, "M","10528519","SANOALO070260", "Militar Activo", "atn_ciudadana@sedena.gob.mx", 200000,8,3),
      ("RETO800313HDFYRS00", "Oscar", "", "Reynoso", "Trejo", 41, "M","C3725910","REYEOOR130380", "Militar Activo", "oscartrejocomisiones@hotmail.com", 12000,9,2),
      ("BABR970908HDFLLL09", "Raul", "Adrian", "Baltazar", "Blancas", 24, "M","D4775872","19-4-17BALAARL", "Militar Activo", "sldrulas@hotmail.com", 10000,10,1),
      ("EACR940718HDFSRB02", "Ruben", "Mauricio", "Esparza", "Carrillo", 27, "M","D2338492","SEPAIRN180794", "Militar Activo", "soldadoesparza123@gmail.com", 10000,11,5),
      ("CIZS480614HDFNPL06", "Salvador", "", "Cienfuegos", "Zepeda", 73, "M","6416991","CINEESR140648", "Militar Retirado", "salvadorcienfuegos@hotmail.com", 190000,12,4),
      ("CUFS861130MDFRSL03", "Silvia", "", "De la Cruz", "Faustino", 35, "F","A10037027","DELAUSA301186", "Militar Activo", "chiborras123@gmail.com", 12000,13,4),
      ("EAAJ780327MMCSBN05", "Juana", "Ofelia", "Escamilla", "Abrego", 43, "M","D4106394","SECAEJA270378", "Militar Activo", "ofeliaescamilla456@outlook.com", 16000,14,3),
      ("PALJ750626MDFLGL09", "Julieta", "", "Palacios", "Lugo", 46, "M","A10029150","PALUOJA260675", "Militar Retirado", "cabojulieta12@hotmail.com", 12000,12,2),
      ("EIMO041220MDFSNRA1", "Orlee", "Adina", "Espinosa", "Montalvo", 17, "F","B0208123","EIMORL201204", "Derechohabiente", "orlee2preciosa@gmail.com", -1,10,1)
    )


    val usuario_column = Seq( "CURP", "Primer_Name", "Segundo_Name", "AP", "AM", "Edad", "Sexo", "Matricula", "Expediente", "Situacion", "email", "sueldo", "grado_id", "vigencia_id")

    val grados = Seq(

      (1,"Soldado","sld"),
      (2,"Cabo","cab"),
      (3,"Sargento Segundo","sgto 2do"),
      (4,"Sargento Primero", "Sgto 1ro"),
      (5,"Subteniente", "STTE"),
      (6,"Teniente", "TTE"),
      (7,"Capitan Segundo", "Cap 2do"),
      (8,"Capitan Primero", "Cap 1ro"),
      (9,"Mayor", "MYR"),
      (10,"Teniente Coronel", "TTE COR"),
      (11,"Coronel", "Cor"),
      (12,"General Brigadier", "Gral Brig"),
      (13,"General de Brigada", "Gral Bgd"),
      (14,"General de Division", "Gral Div"),
      (15,"General Secretario de la Defensa Nacional", "Sec Def Nac"),
      (16,"Comandante Supremo Fuerzas Armadas", "Presidente de la Republica")
    )


    val grados_Column = Seq("grado_id", "Grado_Name", "Abreviatura")

    val empleos = Seq(
      (1,"Auxiliar Informatica","Aux Inftca"),
      (2,"Oficinista","Ofta"),
      (3,"Infanteria", "Inf"),
      (4,"Medico Cirujano","MC"),
      (5,"Arma","Wp"),
      (6,"Materiales de Guerra","War"),
    )

    val empleos_column = Seq("vigencia_id","Empleo","Empleo_Abrev")

    val Militar = usuarios.toDF(usuario_column:_*)
    Militar.show()

    val Posiciones = grados.toDF(grados_Column:_*)
    Posiciones.show()

    val chambitas = empleos.toDF(empleos_column:_*)
    chambitas.show()

    Militar.createOrReplaceTempView("MILITAR")
    Posiciones.createOrReplaceTempView("POSICIONES")
    chambitas.createOrReplaceTempView("CHAMBAS")

    val generales_MC = spark.sql("SELECT Primer_Name, AP, AM, Matricula, Expediente, Abreviatura, Empleo_Abrev FROM Militar as M INNER JOIN POSICIONES as P  ON M.grado_id==P.grado_id JOIN CHAMBAS as C ON M.vigencia_id==C.Vigencia_id where P.grado_id ==12 AND C.Empleo == 'Medico Cirujano'")
    generales_MC.show()

    val TTECOR_INFOR= spark.sql("SELECT Primer_Name, AP, AM, Matricula, Expediente, Abreviatura, Empleo_Abrev FROM MILITAR as M Left JOIN POSICIONES AS P ON M.grado_id==P.grado_id JOIN CHAMBAS as C on M.vigencia_id==C.Vigencia_id where P.grado_id ==10 AND C.Empleo == 'Auxiliar Informatica'")
    TTECOR_INFOR.show()

    val promedios = spark.sql("SELECT  AVG(sueldo) FROM MILITAR where AP =='Espinosa'");
    promedios.show()

    val maximos = spark.sql("SELECT MAX(sueldo) FROM MILITAR");
    maximos.show()

    val minimos = spark.sql("SELECT MIN(sueldo) FROM MILITAR");
    minimos.show()

    val suma = spark.sql("SELECT SUM(sueldo) FROM MILITAR WHERE AP == 'Espinosa'")
    suma.show()


  }
}
