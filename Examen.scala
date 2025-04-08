package job.examen

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions, types}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object examen {

  lazy val spark: SparkSession = SparkSession.builder()
    .appName("Examen")
    .master("local[*]")
    .getOrCreate()

  //Ejercicio 1
  val estudiantes = Seq(
    Row("Maria", 20, 9.1),
    Row("Juan", 22, 7.5),
    Row("Lucia", 19, 8.7),
    Row("Pedro", 21, 6.3),
    Row("Sofia", 23, 9.5)
  )
  val schema = StructType(Seq(
    StructField("Nombre", StringType, nullable = false),
    StructField("Edad", IntegerType, nullable = false),
    StructField("Calificacion", DoubleType, nullable = false)
  ))
  val df = spark.createDataFrame(spark.sparkContext.parallelize(estudiantes), schema)


  def ejercicio1(df: DataFrame)(implicit spark:SparkSession): DataFrame = {
    println("Imprimo esquema")
    df.printSchema()
    println("Imprimo calificaciones mayores a 8")
    df.filter("Calificacion > 8").show()
    println("Imprimo nombres ordenados por calificacion") //si quisiera mostrar solo los nombres tendria que quitar las calificaciones del select
    df.select("Nombre", "Calificacion").orderBy(col("Calificacion").desc).show()
    println("Imprimo dataframe")
    df.show()
    df
  }



  //Ejercicio 2
  def ParImpar(n: Int): String = {
    if (n % 2 == 0) s"$n es par"
    else s"$n es impar"
  }

  def ejercicio2(df: DataFrame)(implicit spark:SparkSession): DataFrame =  {
    val spark = SparkSession
      .builder()
      .appName("ParImparTest")
      .master("local[*]")
      .getOrCreate()
    val ParImparUDF = udf((n: Int) => ParImpar(n))
    val newdf = df.withColumn("Par_Impar", ParImparUDF(df("Edad")))
    newdf.show()
    newdf
  }


  //Ejercicio 3
  val estudiantes1 = Seq(
    Row(1, "Maria"),
    Row(2, "Juan"),
    Row(3, "Lucia"),
    Row(4, "Pedro"),
    Row(5, "Sofia")
  )
  val schema1 = StructType(Seq(
    StructField("ID", IntegerType, nullable = false),
    StructField("Nombre", StringType, nullable = false)
  ))
  val df1 = spark.createDataFrame(spark.sparkContext.parallelize(estudiantes1), schema1)

  val estudiantes2 = Seq(
    Row(1, "Matematicas", 9),
    Row(2, "Literatura", 6),
    Row(3, "Ciencia", 3),
    Row(4, "Historia", 10),
    Row(5, "Economia", 1)
  )
  val schema2 = StructType(Seq(
    StructField("ID_estudiante", IntegerType, nullable = false),
    StructField("Asignatura", StringType, nullable = false),
    StructField("Calificacion", IntegerType, nullable = false)
  ))
  val df2 = spark.createDataFrame(spark.sparkContext.parallelize(estudiantes2), schema2)

  def ejercicio3(df1: DataFrame , df2: DataFrame): DataFrame = {
    val result = df1.join(df2, df1("id") === df2("id_estudiante"))
    result.show()
    result
  }


  //Ejercicio 4
  val lista = List("perro","gato","perro","orca","leon", "gato", "perro", "orca")
  def ejercicio4(palabras: List[String])(implicit spark:SparkSession): RDD[(String, Int)] = {
    val rdd = spark.sparkContext.parallelize(palabras) //convierto la lista en rdd
    val contar = rdd.map(a => (a, 1)).reduceByKey(_ + _)
    contar
  }



   //Ejercicio 5
  val dfcsv = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("C:\\Users\\adril\\Desktop\\Módulos KC\\Data Processing\\Data processing\\Examen\\ventas.csv")

  def ejercicio5(ventas: DataFrame)(implicit spark:SparkSession): DataFrame = {
    val dfConIngreso = dfcsv.withColumn("dineroXid", col("cantidad") * col("precio_unitario"))

    val dineroXProducto = dfConIngreso.groupBy("id_producto").agg(sum("dineroXid").alias("dineroXproducto"))

    val en_orden = dineroXProducto.orderBy(desc("id_producto")) //lo ordeno para que quede mas claro el resultado

    en_orden.show()
    en_orden
  }
}