import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, struct}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.avro.functions._
object AvroProducer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Avro Json data")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("OFF")

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe","Testing-Topic")
      .option("startingOffsets","latest")
      .load()
    /*
        Prints Kafka schema with columns (topic, offset, partition e.t.c)
         */
    df.printSchema()

    val schema = new StructType()
      .add("ID",IntegerType)
      .add("firstname", StringType)
      .add("middlename", StringType)
      .add("lastname", StringType)
      .add("dob_year", IntegerType)
      .add("dob_month", IntegerType)
      .add("gender", StringType)
      .add("salary", IntegerType)

    /*
       Converts JSON string to DataFrame
        */
  val personDF = df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"),schema).as("data"))

    personDF.printSchema()
    /*
        * Convert DataFrame columns to Avro format and name it as "value"
        * And send this Avro data to Kafka topic
        */
    personDF.select(to_avro(struct("data.*"))as "value")
      .writeStream
      .format("kafka")
      .outputMode("append")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("topic","Testing-Topic")
      .option("checkpointLocation","C:\\tmp\\checkpoints")
      .start()
      .awaitTermination()
  }

}
