package scala_jenkins

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.net.HttpURLConnection
import java.net.URL
import scala.io.Source

object sop_read_api {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("MiniPrjScala")
      .enableHiveSupport()
      .getOrCreate()

    // Fetch data from API
    val apiUrl = "https://global-electricity-production.p.rapidapi.com/country?country=Afghanistan"
    val connection = new URL(apiUrl).openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestMethod("GET")
    connection.setRequestProperty("x-rapidapi-key", "48bce046c0mshee45259ba8a9955p1a871bjsn90680f9cecd6")
    connection.setRequestProperty("x-rapidapi-host", "global-electricity-production.p.rapidapi.com")

    val apiData = try {
      val inputStream = connection.getInputStream
      val data = Source.fromInputStream(inputStream).mkString
      inputStream.close()
      data
    } finally {
      connection.disconnect()
    }

    // Define the schema of the API response (update schema according to API response structure)
    val schema = StructType(Seq(
      StructField("country", StringType, true),
      StructField("code", StringType, true),
      StructField("year", IntegerType, true),
      StructField("coal", DoubleType, true),
      StructField("gas", DoubleType, true),
      StructField("hydro", DoubleType, true),
      StructField("other_renewables", DoubleType, true),
      StructField("solar", DoubleType, true),
      StructField("oil", DoubleType, true),
      StructField("wind", DoubleType, true),
      StructField("nuclear", DoubleType, true)
    ))

    // Parse JSON and create a DataFrame
    import spark.implicits._
    val df = spark.read.schema(schema).json(Seq(apiData).toDS())

    println(df.printSchema())
    println(df.show(10))
    println("Automated")

    // Add transformations or calculations based on API data
    val df_transformed = df
      .withColumn("total_renewables", col("hydro") + col("other_renewables") + col("solar") + col("wind"))
      .withColumn("renewable_percentage", col("total_renewables") /
        (col("coal") + col("gas") + col("oil") + col("nuclear") + col("total_renewables")) * 100)

    df_transformed.show(10)

    // Sort the DataFrame by year
    val sorted_df = df_transformed.orderBy("year")
    sorted_df.show(10)

    sorted_df.write.mode("overwrite").saveAsTable("product.electricity_data")
    println("In Hive")
  }
}
