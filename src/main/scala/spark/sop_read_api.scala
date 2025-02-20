package spark //change it to your directory name
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import requests._
object sop_read_api {

  def main(args: Array[String]): Unit = {
    //Start spark session
    val spark = SparkSession.builder()
      .appName("My Spark Application")
      .master("local[*]")
      .getOrCreate()
    while (true) {
      import spark.implicits._
    //API details loading
      val apiUrl = "https://api.tfl.gov.uk/Line/victoria/Arrivals?app_id=92293faa428041caad3dd647d39753a0&app_key=ba72936a3db54b4ba5792dc8f7acc043"
      val response = get(apiUrl, headers = headers)
      val total = response.text()
      val dfFromText = spark.read.json(Seq(total).toDS)

      // select the columns you want to include in the message

      val messageDF = dfFromText.select($"id", $"stationName", $"lineName", $"towards",
        $"expectedArrival",$"vehicleId",$"platformName",$"direction",$"destinationName",
        $"timestamp",$"timeToStation", $"currentLocation",$"timeToLive")
      // Show a few messages, e.g., 5 rows
      messageDF.show(5, truncate = false)

      //Kafka server and topic name assignment
      val kafkaServer: String = "ip-172-31-8-235.eu-west-2.compute.internal:9092"
      val topicSampleName: String = "sujay_topic1" //your topic name

      messageDF.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value").selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").write.format("kafka").option("kafka.bootstrap.servers", kafkaServer).option("topic", topicSampleName).save()
      println("message is loaded to kafka topic")
      Thread.sleep(10000) // wait for 10 seconds before making the next call

    }
  }

}

//mvn package
//spark-submit --master local --packages "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7","com.lihaoyi:requests_2.11:0.7.1" --class spark.sop_read_api target/SOP_API-1.0-SNAPSHOT.jar