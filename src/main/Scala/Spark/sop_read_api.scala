import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import requests._
object sop_read_api {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("My Spark Application")
      .master("local[*]")
      .getOrCreate()
    while (true) {
      import spark.implicits._
      val apiUrl = "https://api.tfl.gov.uk/Line/victoria/Arrivals?app_id=92293faa428041caad3dd647d39753a0&app_key=ba72936a3db54b4ba5792dc8f7acc043"
      val response = get(apiUrl, headers = headers)
      val total = response.text()
      val dfFromText = spark.read.json(Seq(total).toDS)

      // select the columns you want to include in the message

      val messageDF = dfFromText.select($"id", $"stationName", $"lineName", $"towards",
        $"expectedArrival",$"vehicleId",$"platformName",$"direction",$"destinationName",
        $"timestamp",$"timeToStation", $"currentLocation",$"timeToLive")
      print(messageDF)

    }
  }

}