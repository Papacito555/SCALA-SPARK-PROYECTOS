package com.TarantulaStudios.spark.RDD

import org.apache.spark.ml.attribute.{Attribute, NumericAttribute}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType

object Separar_Datos {

  /**
   * Created by shirukai on 2018/9/12
   * Columnas divididas
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    // Crear un DataFrame desde la memoria
    val df = Seq("Ming,20,15552211521", "hong,19,13287994007", "zhi,21,15552211523")
      .toDF("value")
    df.show()

    /**
     * +-------------------+
     * |              value|
     * +-------------------+
     * |Ming,20,15552211521|
     * |hong,19,13287994007|
     * | zhi,21,15552211523|
     * +-------------------+
     */

    import org.apache.spark.sql.functions._
    // Método 1: use la función integrada dividir y luego iterar a través de las columnas
    val separator = ","
    lazy val first = df.first()

    val numAttrs = first.toString().split(separator).length
    val attrs = Array.tabulate(numAttrs)(n => "col_" + n)
    // Divide la columna de valor por el delimitador especificado para generar la columna splitCols
    var newDF = df.withColumn("splitCols", split($"value", separator))
    attrs.zipWithIndex.foreach(x => {
      newDF = newDF.withColumn(x._1, $"splitCols".getItem(x._2))
    })
    newDF.show()

    /**
     * +-------------------+--------------------+-----+-----+-----------+
     * |              value|           splitCols|col_0|col_1|      col_2|
     * +-------------------+--------------------+-----+-----+-----------+
     * |Ming,20,15552211521|[Ming, 20, 155522...| Ming|   20|15552211521|
     * |hong,19,13287994007|[hong, 19, 132879...| hong|   19|13287994007|
     * | zhi,21,15552211523|[zhi, 21, 1555221...|  zhi|   21|15552211523|
     * +-------------------+--------------------+-----+-----+-----------+
     */

    // Método 2: Use la función udf para crear múltiples columnas y luego fusionar
    val attributes: Array[Attribute] = {
      val numAttrs = first.toString().split(separator).length
      // Generar atributos
      Array.tabulate(numAttrs)(i => NumericAttribute.defaultAttr.withName("value" + "_" + i))
    }
    // Crea múltiples columnas de datos
    val fieldCols = attributes.zipWithIndex.map(x => {
      val assembleFunc = udf {
        str: String =>
          str.split(separator)(x._2)
      }
      assembleFunc(df("value").cast(StringType)).as(x._1.name.get, x._1.toMetadata())
    })
    // Fusionar datos
    df.select(col("*") +: fieldCols: _*).show()

    /**
     * +-------------------+-------+-------+-----------+
     * |              value|value_0|value_1|    value_2|
     * +-------------------+-------+-------+-----------+
     * |Ming,20,15552211521|   Ming|     20|15552211521|
     * |hong,19,13287994007|   hong|     19|13287994007|
     * | zhi,21,15552211523|    zhi|     21|15552211523|
     * +-------------------+-------+-------+-----------+
     */
  }


}
