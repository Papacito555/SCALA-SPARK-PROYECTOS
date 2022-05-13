package com.TarantulaStudios.spark.SEDENAEJEMPLOSWORK

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter, RightOuter}


object Joins {

  def main(args:Array[String]): Unit ={

    val spark =SparkSession.builder()
      .master("local[*]")
      .appName("Joins")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val militar = Seq(
      (1,"Antonio", "", "Garcia", "Ruiz", 12, "Lope de vega", "209", "Polanco V Secc", "11560","Ciudad de Mexico"),
      (2,"Jacinta", "Mclovin", "Espinosa", "Montalvo", 14, "Sierra Ventanas", "650", "Lomas de Chapultepec", "11000","Ciudad de Mexico"),
      (3,"Manuel", "Alberto", "Mijares", "Lopez", 1,  "Avenida Molliere", "222", "Polanco II Secc", "11550","Ciudad de Mexico"),
      (4,"Armando", "", "Bazan", "Martinez",8, "Gallo Colorado", "46", "Benito Juarez II Secc", "57000","Estado de Mexico"),
      (5,"Jason", "Joel", "Desrouleaux", "",6, "Bahía de Ballenas", "S/N", "Veronica Anzures", "11300","Ciudad de Mexico"),
      (6,"Porfirio", "El Pollo", "Espinosa", "Bazan",13,  "Av. Paseo de la Reforma", "510", "Juarez", "06600","Ciudad de Mexico"),
      (7,"Dolores", "", "Montalvo", "Correa",9, "Av. Paseo de la Reforma", "509", "Juarez", "06600","Ciudad de Mexico"),
      (8,"Luis", "Mario", "Jimenez", "Bautista", 4, "Hermelegildo Galeana", "181", "San Jeronimo 4 Vientos", "56531","Ciudad de Mexico"),
      (9,"Raul", "", "Hernandez", "Martinez", 3,  "Calle 10", "370", "Esperanza", "57800","Estado de Mexico"),
      (10,"Abel", "", "Makkonen", "Tesfaye", 8, "Rexford Dr", "258", "Beverly Hills", "90212","California"),
      (11,"Pinto ", "Madafaka", "Espinosa", "Lorca",2, "Hollywood Sign", "S/N", "Hollywood", "90068","California"),
      (12,"Javier", "", "Lopez", "Chabelo", -1,  "Ardilla", "100", "Benito Juarez I Secc", "57000","Estado de Mexico"),
      (13,"Norberto", "", "Vicente", "Velazco", 10, "Tacos de Canasta", "304", "El Sol", "11560","Estado de Mexico"),
      (14,"Luis", "Cresencio", "Sandoval", "Gonzalez",11, "Avenida Constituyentes", "09", "Tacubaya", "11560","Ciudad de Mexico"),
      (15,"Eduardo", "Angel", "Espinosa", "Montalvo", 15,  "Av. Palacio Nacional", "S/N", "Centro Historico", "00500","Ciudad de Mexico"),
      (16,"Harry", "Edward", "Styles", "", 7, "Periférico Blvrd Manuel Ávila Camacho", "S/N", "PMilitar", "11200","Ciudad de Mexico"),
      (17,"Nicholas", "Jerry", "Jonas", "",6, " Wilshire Blvd", "100", "Santa Monica", "90401","California"),
      (18,"Manuel", "", "Avila", "Camacho", 5,  "Londres", "247", "Del Carmen", "04100","Ciudad de Mexico"),
      (19,"Armando", "Vincent", "Vega", "Baez",4, "Calz. de Tlalpan", "3465", "Sta. Úrsula Coapa", "04650","Ciudad de Mexico"),
      (20,"Ariana", "", "Grande", "Butera",3, " Woodrow Wilson Dr", "7334", "Hollywood Hils", "90046","California"),
      (21,"Jose de la Cruz", "Porfirio", "Diaz", "Mori",8,  "Bosque de Chapultepec I Secc", "S/N", "Bosque de Chapultepec I Secc", "11100","Ciudad de Mexico"),
      (22,"Emmanuel", "", "Cruz", "Colin",9, "Caballo Bayo", "110", "Benito Juarez", "57800","Estado  de Mexico"),
      (23,"Edgar", "Armando", "Trejo", "Gutierrez", 4, "Verano", "196", "San Jacinto 4 Vientos", "52766","Estado de Mexico"),
      (24,"Edwing", "David", "Avila", "Tovar", 3,  "Fray Juan de Zumárraga", "2", "Villa Gustavo A. Madero", "07050","Ciudad de Mexico"),
      (25,"Esteban", "Jassiel", "Alvarez", "Preciado", 2, "Av. Tepozanes", "3", "Valle de los Reyes", "56430","Estado de Mexico"),
      (26,"Marco", "Antonio", "Alcantara", "Noriega",-1, "Av Hacienda de Rancho Seco", "S/N", "Plazas de Aragon", "57171","Estado de Mexico"),
      (27,"Jose", "Leandro", "Jimenez", "Acosta", -1,  "Oeste", "1", "Rosa de Castilla", "53520","Estado de Mexico"),
      (28,"Gilberto", "", "Antonio", "Resendiz", 10, "Anillo Periferico", "401", "Lomas Hermosa", "11200","Ciudad de Mexico"),
      (29,"Lorenzo", "", "Cruz", "Ramos",-1, "Av. José Martí", "142", "Escandón I Secc", "11800","Ciudad de Mexico"),
      (30,"Nolan", "Alberto", "Garcia", "Ruiz", -1,  "Av. Revolución", "241", "Tacubaya", "11870","Ciudad de Mexico"),
      (31,"Oscar", "", "Reynoso", "Trejo",6, "La Castañeda", "5", "Mixcoac", "03910","Ciudad de Mexico"),
      (32,"Luis", "Antonio", "Espinosa", "Garcia", 7, "Santa Monica Pier", "200", "Santa Monica", "90401","Ciudad de Mexico"),
      (33,"Luis", "David", "Fernandez", "Molina", 4,  "Avenida Molliere", "262", "Polanco II Secc", "11550","Ciudad de Mexico"),
      (34,"Gabriel", "Bryan", "Perez", "Torres",3, "Ermita Iztapalapa", "557", "Granjas Esmeralda", "09810","Ciudad de Mexico"),
      (35,"Raymundo", "", "Martinez", "Compean",2, "Calle 3", "123", "Agrícola Pantitlán", "08100","Ciudad de Mexico"),
      (36,"Nohemi", "", "Velez", "Ortega",5,  "Av. de las Palomas", "610", "Juarez", "06600","Ciudad de Mexico"),
      (37,"Jose", "Luis", "Ramirez", "Hernandez",3, "Av. Lazaro Cardenas", "509", "Benito Juarez", "57000","Estado de Mexico"),
      (38,"Benito", "", "Juarez", "Garcia", 4, "Av Riva Palacio", "94", "Estado de Mexico", "57210","Ciudad de Mexico"),
      (39,"Manuel", "", "Luna", "Martinez", 3,  "Enramada", "240", "Benito Juarez", "57000","Estado de Mexico"),
      (40,"Franklin", "", "Clinton", "Johnson", 8, "Ocean Front Walk", "1800", "Venice", "90291","California"),
      (41,"", "Madafaka", "Espinosa", "Lorca",9, "Hollywood Sign", "S/N", "Hollywood", "90068","California"),
      (42,"Javier", "", "Lopez", "Chabelo", -1,  "Ardilla", "100", "Benito Juarez I Secc", "57000","Estado de Mexico"),
      (43,"Norberto", "", "Vicente", "Velazco", 6, "Tacos de Canasta", "304", "El Sol", "11560","Estado de Mexico"),
      (44,"Luis", "Cresencio", "Sandoval", "Gonzalez",1, "Avenida Constituyentes", "09", "Tacubaya", "11560","Ciudad de Mexico"),
      (45,"Eduardo", "Angel", "Espinosa", "Montalvo", 5,  "Av. Palacio Nacional", "S/N", "Centro Historico", "00500","Ciudad de Mexico"),
      (46,"Antonio", "", "Garcia", "Ruiz", 2, "Lope de vega", "209", "Polanco V Secc", "11560","Ciudad de Mexico"),
      (47,"Jacinta", "Mclovin", "Espinosa", "Montalvo", 7, "Sierra Ventanas", "650", "Lomas de Chapultepec", "11000","Ciudad de Mexico"),
      (48,"Manuel", "Alberto", "Mijares", "Lopez",9,  "Avenida Molliere", "222", "Polanco II Secc", "11550","Ciudad de Mexico"),
      (49,"Armando", "", "Bazan", "Martinez",4, "Gallo Colorado", "46", "Benito Juarez II Secc", "57000","Estado de Mexico"),
      (50,"Jason", "Joel", "Desrouleaux", "",5, "Bahía de Ballenas", "S/N", "Veronica Anzures", "11300","Ciudad de Mexico"),
      (51,"Porfirio", "El Pollo", "Espinosa", "Bazan",13,  "Av. Paseo de la Reforma", "510", "Juarez", "06600","Ciudad de Mexico"),
      (52,"Dolores", "", "Montalvo", "Correa",9, "Av. Paseo de la Reforma", "509", "Juarez", "06600","Ciudad de Mexico"),
      (53,"Luis", "Mario", "Jimenez", "Bautista", 4, "Hermelegildo Galeana", "181", "San Jeronimo 4 Vientos", "56531","Ciudad de Mexico"),
      (54,"Raul", "", "Hernandez", "Martinez", 3,  "Calle 10", "370", "Esperanza", "57800","Estado de Mexico"),
      (55,"Abel", "", "Makkonen", "Tesfaye", 8, " Rexford Dr", "258", "Beverly Hills", "90212","California"),
      (56,"Pinto ", "Madafaka", "Espinosa", "Lorca",2, "Hollywood Sign", "S/N", "Hollywood", "90068","California"),
      (57,"Javier", "", "Lopez", "Chabelo", -1,  "Ardilla", "100", "Benito Juarez I Secc", "57000","Estado de Mexico"),
      (58,"Norberto", "", "Vicente", "Velazco", 10, "Tacos de Canasta", "304", "El Sol", "11560","Estado de Mexico"),
      (59,"Luis", "Cresencio", "Sandoval", "Gonzalez",11, "Avenida Constituyentes", "09", "Tacubaya", "11560","Ciudad de Mexico"),
      (60,"Eduardo", "Angel", "Espinosa", "Montalvo", 15,  "Av. Palacio Nacional", "S/N", "Centro Historico", "00500","Ciudad de Mexico"),
      (61,"Antonio", "", "Garcia", "Ruiz", 12, "Lope de vega", "209", "Polanco V Secc", "11560","Ciudad de Mexico"),
      (62,"Jacinta", "Mclovin", "Espinosa", "Montalvo", 14, "Sierra Ventanas", "650", "Lomas de Chapultepec", "11000","Ciudad de Mexico"),
      (63,"Manuel", "Alberto", "Mijares", "Lopez", 1,  "Avenida Molliere", "222", "Polanco II Secc", "11550","Ciudad de Mexico"),
      (64,"Armando", "", "Bazan", "Martinez",8, "Gallo Colorado", "46", "Benito Juarez II Secc", "57000","Estado de Mexico"),
      (65,"Jason", "Joel", "Desrouleaux", "",6, "Bahía de Ballenas", "S/N", "Veronica Anzures", "11300","Ciudad de Mexico"),
      (66,"Porfirio", "El Pollo", "Espinosa", "Bazan",13,  "Av. Paseo de la Reforma", "510", "Juarez", "06600","Ciudad de Mexico"),
      (67,"Dolores", "", "Montalvo", "Correa",9, "Av. Paseo de la Reforma", "509", "Juarez", "06600","Ciudad de Mexico"),
      (68,"Luis", "Mario", "Jimenez", "Bautista", 4, "Hermelegildo Galeana", "181", "San Jeronimo 4 Vientos", "56531","Ciudad de Mexico"),
      (69,"Raul", "", "Hernandez", "Martinez", 3,  "Calle 10", "370", "Esperanza", "57800","Estado de Mexico"),
      (70,"Abel", "", "Makkonen", "Tesfaye", 8, " Rexford Dr", "258", "Beverly Hills", "90212","California"),
      (71,"Pinto ", "Madafaka", "Espinosa", "Lorca",2, "Hollywood Sign", "S/N", "Hollywood", "90068","California"),
      (72,"Javier", "", "Lopez", "Chabelo", -1,  "Ardilla", "100", "Benito Juarez I Secc", "57000","Estado de Mexico"),
      (73,"Norberto", "", "Vicente", "Velazco", 10, "Tacos de Canasta", "304", "El Sol", "11560","Estado de Mexico"),
      (74,"Luis", "Cresencio", "Sandoval", "Gonzalez",11, "Avenida Constituyentes", "09", "Tacubaya", "11560","Ciudad de Mexico"),
      (75,"Eduardo", "Angel", "Espinosa", "Montalvo", 15,  "Av. Palacio Nacional", "S/N", "Centro Historico", "00500","Ciudad de Mexico"),
      (76,"Antonio", "", "Garcia", "Ruiz", 12, "Lope de vega", "209", "Polanco V Secc", "11560","Ciudad de Mexico"),
      (77,"Jacinta", "Mclovin", "Espinosa", "Montalvo", 14, "Sierra Ventanas", "650", "Lomas de Chapultepec", "11000","Ciudad de Mexico"),
      (78,"Manuel", "Alberto", "Mijares", "Lopez", 1,  "Avenida Molliere", "222", "Polanco II Secc", "11550","Ciudad de Mexico"),
      (79,"Armando", "", "Bazan", "Martinez",8, "Gallo Colorado", "46", "Benito Juarez II Secc", "57000","Estado de Mexico"),
      (80,"Jason", "Joel", "Desrouleaux", "",6, "Bahía de Ballenas", "S/N", "Veronica Anzures", "11300","Ciudad de Mexico"),
      (81,"Porfirio", "El Pollo", "Espinosa", "Bazan",13,  "Av. Paseo de la Reforma", "510", "Juarez", "06600","Ciudad de Mexico"),
      (82,"Dolores", "", "Montalvo", "Correa",9, "Av. Paseo de la Reforma", "509", "Juarez", "06600","Ciudad de Mexico"),
      (83,"Luis", "Mario", "Jimenez", "Bautista", 4, "Hermelegildo Galeana", "181", "San Jeronimo 4 Vientos", "56531","Ciudad de Mexico"),
      (84,"Raul", "", "Hernandez", "Martinez", 3,  "Calle 10", "370", "Esperanza", "57800","Estado de Mexico"),
      (85,"Abel", "", "Makkonen", "Tesfaye", 8, " Rexford Dr", "258", "Beverly Hills", "90212","California"),
      (86,"Pinto ", "Madafaka", "Espinosa", "Lorca",2, "Hollywood Sign", "S/N", "Hollywood", "90068","California"),
      (87,"Javier", "", "Lopez", "Chabelo", -1,  "Ardilla", "100", "Benito Juarez I Secc", "57000","Estado de Mexico"),
      (88,"Norberto", "", "Vicente", "Velazco", 10, "Tacos de Canasta", "304", "El Sol", "11560","Estado de Mexico"),
      (89,"Luis", "Cresencio", "Sandoval", "Gonzalez",11, "Avenida Constituyentes", "09", "Tacubaya", "11560","Ciudad de Mexico"),
      (90,"Eduardo", "Angel", "Espinosa", "Montalvo", 15,  "Av. Palacio Nacional", "S/N", "Centro Historico", "00500","Ciudad de Mexico"),
      (91,"Antonio", "", "Garcia", "Ruiz", 12, "Lope de vega", "209", "Polanco V Secc", "11560","Ciudad de Mexico"),
      (92,"Jacinta", "Mclovin", "Espinosa", "Montalvo", 14, "Sierra Ventanas", "650", "Lomas de Chapultepec", "11000","Ciudad de Mexico"),
      (93,"Manuel", "Alberto", "Mijares", "Lopez", 1,  "Avenida Molliere", "222", "Polanco II Secc", "11550","Ciudad de Mexico"),
      (94,"Armando", "", "Bazan", "Martinez",8, "Gallo Colorado", "46", "Benito Juarez II Secc", "57000","Estado de Mexico"),
      (95,"Jason", "Joel", "Desrouleaux", "",6, "Bahía de Ballenas", "S/N", "Veronica Anzures", "11300","Ciudad de Mexico"),
      (96,"Porfirio", "El Pollo", "Espinosa", "Bazan",13,  "Av. Paseo de la Reforma", "510", "Juarez", "06600","Ciudad de Mexico"),
      (97,"Dolores", "", "Montalvo", "Correa",9, "Av. Paseo de la Reforma", "509", "Juarez", "06600","Ciudad de Mexico"),
      (98,"Luis", "Mario", "Jimenez", "Bautista", 4, "Hermelegildo Galeana", "181", "San Jeronimo 4 Vientos", "56531","Ciudad de Mexico"),
      (99,"Raul", "", "Hernandez", "Martinez", 3,  "Calle 10", "370", "Esperanza", "57800","Estado de Mexico"),
      (100,"Abel", "", "Makkonen", "Tesfaye", 8, " Rexford Dr", "258", "Beverly Hills", "90212","California"),

    )

    val militarcolumns = Seq("militar_id","primer_nombre","Segundo_Nombre", "AP", "AM", "mil_grado_id", "calle","Numerodomicilio",
      "colonia","CP","estado")

    import spark.sqlContext.implicits._

    val MilDF = militar.toDF(militarcolumns:_*)
    MilDF.show(false)

    /*Tabla General */
    MilDF.sort($"primer_nombre".asc).repartition(1).write
      .mode(SaveMode.Overwrite)
      .option("header","True")
      .csv("C:/Users/SDS-Usuario/Downloads/spark/data/data/Militares")


    val grado = Seq(
      (1, "Soldado",  "Informatica","Info"),
      (2, "Cabo", "Oficinista","Ofc"),
      (3, "Sargento Segundo",  "Derecho","Der"),
      (4, "Sargento Primero", "Policia Militar","PM"),
      (5, "Subteniente",  "Enfermera","Enf"),
      (6, "Teniente", "Bailarin","Bail"),
      (7, "Capitan Segundo",  "Administrador","Admon"),
      (8, "Capitan Primero", "Transmisiones","Trans"),
      (9, "Mayor",  "Cocinero","Coc"),
      (10, "Teniente Coronel", "Estado Mayor","EM"),
      (11, "Coronel",  "Pagaduria","Pag"),
      (12, "General Brigadier", "MedicO Cirujano","MC"),
      (13, "General de Brigada", "Intendente", "Int"),
      (14, "General de Division", "Diplomado Estado Mayor","DEM"),
      (15, "Presidente de la Republica", "Presidente", "Pres")

    )

    val gradoColumns = Seq("grado_id","grado_name", "empleo_name","empleo_abrev")
    val gradoDF = grado.toDF(gradoColumns:_*)
    gradoDF.show(false)


    //Consultas JOIN
    println("Inner Join")
    MilDF.join(gradoDF,MilDF("mil_grado_id") ===  gradoDF("grado_id"),"inner")
      .show(false)

    println("Outer Join")
    MilDF.join(gradoDF,MilDF("mil_grado_id") ===  gradoDF("grado_id"),"outer")
      .show(false)

    println("Full Join")
    MilDF.join(gradoDF,MilDF("mil_grado_id") ===  gradoDF("grado_id"),"full")
      .show(false)

    println("Full Outer Join")
    MilDF.join(gradoDF,MilDF("mil_grado_id") ===  gradoDF("grado_id"),"fullouter")
      .show(false)

    //Left Join
    println("Left Join")
    MilDF.join(gradoDF,MilDF("mil_grado_id") ===  gradoDF("grado_id"),"left")
      .show(false)

    println("Left Outer Join")
    MilDF.join(gradoDF,MilDF("mil_grado_id") ===  gradoDF("grado_id"),"leftouter")
      .show(false)

    //Right Join
    println("Right Join")
    MilDF.join(gradoDF,MilDF("mil_grado_id") ===  gradoDF("grado_id"),"right")
      .show(false)

    println("Right Outer Join")
    MilDF.join(gradoDF,MilDF("mil_grado_id") ===  gradoDF("grado_id"),"rightouter")
      .show(false)

    //Semi Joins
    println("Left Semi Join")
    MilDF.join(gradoDF,MilDF("mil_grado_id") ===  gradoDF("grado_id"),"leftsemi")
      .show(false)

    println("Left Anti Join")
    MilDF.join(gradoDF,MilDF("mil_grado_id") ===  gradoDF("grado_id"),"leftanti")
      .show(false)

    //Joins con SQL

    MilDF.createOrReplaceTempView("MILITAR")
    gradoDF.createOrReplaceTempView("GRADO")

    val joinDF = spark.sql("SELECT primer_nombre,Segundo_Nombre, AP, AM,grado_name, empleo_abrev FROM MILITAR m INNER JOIN Grado g on m.mil_grado_id == g.grado_id")
    joinDF.show(false)

    //Imprimir Militares y Grados
    joinDF.sort($"AP",$"AM",$"primer_nombre".asc).repartition(1 ).write
      .mode(SaveMode.Overwrite)
      .option("header", "True")
      .csv("C:/Users/SDS-Usuario/Downloads/Militares/Generales")



    val joinDF2 = spark.sql("SELECT primer_nombre, segundo_nombre, AP, AM, grado_name, empleo_abrev, estado FROM MILITAR M LEFT JOIN GRADO G ON M.mil_grado_id == G.grado_id where AP == 'Espinosa' ORDER BY AM")
    joinDF2.show(false)
    //Personas con Apellido Espinosa
    joinDF2.sort($"AP",$"AM",$"primer_nombre".asc).repartition(1).write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv("C:/Users/SDS-Usuario/Downloads/Militares/Espinosa")

    val joinDF3 = spark.sql("SELECT primer_nombre, AP, grado_name, empleo_name, cp, estado FROM MILITAR M RIGHT JOIN GRADO G ON M.mil_grado_id == G.grado_id where estado == 'Ciudad de Mexico' order by AP")
    joinDF3.show(false)

    joinDF3.sort($"AP",$"primer_nombre".asc).repartition(1).write
      .mode(SaveMode.Overwrite)
      .option("header","true")
      .csv("C:/Users/SDS-Usuario/Downloads/Militares/Name")

    //Anti Join SQL
    val joinDF4 = spark.sql("SELECT primer_nombre, AP, AM, estado FROM MILITAR m ANTI JOIN GRADO g ON M.mil_grado_id == G.grado_id ")
    joinDF4.show(false)

    val joinDF5 = spark.sql("SELECT primer_nombre, segundo_nombre, AP, grado_name, empleo_abrev, CP FROM MILITAR M LEFT JOIN GRADO G ON M.mil_grado_id == G.grado_id where CP >='57000' order by AP, primer_nombre")
    joinDF5.show(false)

    joinDF5.sort($"AP",$"primer_nombre".asc).repartition(1).write
      .mode(SaveMode.Overwrite)
      .option("header","true")
      .csv("C:/Users/SDS-Usuario/Downloads/Militares/CODIGO_POSTAL")

    val joinDF6 = spark.sql("SELECT primer_nombre, segundo_nombre, AP, grado_name, empleo_abrev FROM MILITAR M FULL OUTER JOIN GRADO G ON M.mil_grado_id == G.grado_id where grado_name == 'General de Division' OR grado_name =='General Brigadier' OR grado_name =='General de Brigada' order by AP,primer_nombre")
    joinDF6.show(false)

    joinDF6.sort($"AP",$"primer_nombre").repartition(1).write
      .mode(SaveMode.Overwrite)
      .option("header","true")
      .csv("C:/Users/SDS-Usuario/Downloads/Militares/Generales")


  }
}




/*Churro


 */