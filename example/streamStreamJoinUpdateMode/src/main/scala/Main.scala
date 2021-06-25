import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.{functions => F}
import java.sql.Timestamp
        
object Main {

    val trigger_interval = "30 seconds"

    def main(args: Array[String]): Unit = {
        lazy val spark = SparkSession.builder().master("local[4]").getOrCreate()

        /**
         * state files will be created for each partition
         * adjust this can avoid creating states for empty partitions
         */
        spark.conf.set("spark.sql.shuffle.partitions", 1)

        import spark.implicits._

        /*
        Rate source (for testing): Generates data at the specified number of rows per second, each output row contains
        a timestamp and value:
          - timestamp is a Timestamp type containing the time of message dispatch,
          - value is of Long type containing the message count, starting from 0 as the first row.
         */
        val ds1 = spark.readStream
                .format("rate")
                .option("numPartitions", 2)
                .option("rowsPerSecond", 1)
                .option("rampUpTime", 3)
                .load()
                .withColumnRenamed("timestamp", "timestamp1")
                .withColumnRenamed("value", "id1")

        val ds2 = spark.readStream
          .format("rate")
          .option("numPartitions", 2)
          .option("rowsPerSecond", 1)
          .option("rampUpTime", 3)
          .load()
          .withColumnRenamed("timestamp", "timestamp2")
          .withColumnRenamed("value", "id2")
          .as[Input2]
          .map { (x: Input2) => Input2(new Timestamp(x.timestamp2.getTime + 20*1000L), x.id2) }

        val ds1WithWatermark = ds1.withWatermark("timestamp1", "5 seconds")
        val ds2WithWatermark = ds2.withWatermark("timestamp2", "10 seconds")

        val result = ds1WithWatermark.join(
            ds2WithWatermark,
            F.expr(
                """
                  | id1 = id2 AND
                  | timestamp2 >= timestamp1 AND
                  | timestamp2 <= timestamp1 + interval 30 seconds
                  |""".stripMargin),
            joinType = "leftOuter"
        )

        val query = result.writeStream
          .format("console")
          .option("numRows", 100)
          .option("truncate", false)
          .outputMode("update")
          .trigger(Trigger.ProcessingTime(trigger_interval))
          .start()

        query.awaitTermination()
    }
}

/*
Exception in thread "main" org.apache.spark.sql.AnalysisException: Join between two streaming DataFrames/Datasets is not supported in Update output mode, only in Append output mode;;
 */