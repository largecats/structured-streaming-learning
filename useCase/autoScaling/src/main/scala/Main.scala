import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{sources, DataFrame, SparkSession}

import scala.collection.mutable.Map
import scala.math.Integral.Implicits._
import org.json4s._ // If dependency has issues, can try using https://github.com/apache/spark/blob/8c69e9cd944415aaf301163128306f678e5bbcca/external/kafka-0-10-sql/src/main/scala/org/apache/spark/sql/kafka010/JsonUtils.scala
import org.json4s.native.Serialization._
import org.json4s.native.Serialization

/**
  * Based on https://confluence.garenanow.com/display/DATA/2021-03+Migrate+SparkStreaming+to+StructuredStreaming#id-202103MigrateSparkStreamingtoStructuredStreaming-FolderStructureDifference
  *
  * 1. Read stream from Kafka producer.
  * 2. For each microbatch, split it into partitions where each partition contains no more than SPLIT_SIZE rows. The splitting is done by re-reading from the Kafka source with:
  *    i. the microbatch's original starting and ending offsets, and
  *    ii. minPartitions, calculated from the microbatch's original partition sizes.
  *
  *  To test in spark-shell:
  *  1. Launch spark-shell using /usr/local/spark3.0.1/bin/spark-shell --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.json4s:json4s-native_2.12:3.6.6
  *  2. Load the script using > :load /home/datadev/xiaolf/structured-streaming-learning/autoScaling/src/main/scala/Main.scala
  *  3. Run the script using > Main.main(Array[String]())
  */
object Main {

  val SPLIT_SIZE = 1000L // 1000 records in a partition
  val TRIGGER_INTERVAL = "20 seconds"
  implicit val formats = Serialization.formats(NoTypeHints)

  def main(args: Array[String]): Unit = {
//    lazy val spark = SparkSession.builder().master("local[4]").getOrCreate()
    lazy val spark = SparkSession.builder.getOrCreate()

    // Read from Kafka producer
    val streamingDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "data-kafka00:8920,data-kafka01:8920,data-kafka02:8920")
      .option("subscribe", "test0,test1,test2")
      .option("startingOffsets", "earliest")
      .load()

    // Split each microbatch using minPartitions
    def splitBatch(
      df: DataFrame,
      epochId: Long
    ): Unit = { // Function to put in foreachBatch http://spark.apache.org/docs/3.0.1/structured-streaming-programming-guide.html#foreachbatch
      val assignJson = Map[String, List[Int]]() // Map of topic -> list of partitions
      val startJson = Map[String, Map[Int, Long]]() // Map of topic -> (Map of partition -> startOffset)
      val endJson = Map[String, Map[Int, Long]]() // Map of topic -> (Map of partition -> endOffset)
      var size = 0L
      var numParts = 0L // Value for minPartitions calculated from df's partitions
      for(p <- df.rdd.partitions) {
        val offset = getFields(getFields(p, "inputPartition"), "offsetRange") // this inputPartition is an additional wrap for streamDF, batchDF can use p.offsetRange() directly
        // Get information about the partition offset
        val offsetSize = getMethods(offset, "size").asInstanceOf[Long]
        val topic = getMethods(offset, "topic").asInstanceOf[String]
        val partition = getMethods(offset, "partition").asInstanceOf[Int]
        val fromOffset = getMethods(offset, "fromOffset").asInstanceOf[Long]
        val untilOffset = getMethods(offset, "untilOffset").asInstanceOf[Long]
        assignJson.get(topic) match {
          case Some(e) => assignJson.update(topic, e :+ partition)
          case None    => assignJson.update(topic, List(partition))
        }
        startJson.get(topic) match {
          case Some(e) => e.update(partition, fromOffset)
          case None    => startJson.update(topic, Map(partition -> fromOffset))
        }
        endJson.get(topic) match {
          case Some(e) => e.update(partition, untilOffset)
          case None    => endJson.update(topic, Map(partition -> untilOffset))
        }
        size += offsetSize
        numParts += (offsetSize /% SPLIT_SIZE)._1 + 1L // /% produces (quotient, remainder)
        println(
          s"topic = ${topic}\npartition = ${partition}\noffset = ${offset}\noffsetSize = ${offsetSize}\nfromOffset = ${fromOffset}\nuntilOffset = ${untilOffset}\nsize = ${size}\nnumParts = ${numParts}"
        )
        println(s"assignJson = ${assignJson}\nstartJson=${startJson}\nendJson=${endJson}")
      }
      // additional initial batch might be created to query starting offsets if not specific (e.g. 'earliest', 'latest'), ignore this batch
      if(assignJson.nonEmpty && startJson.nonEmpty && endJson.nonEmpty) {
        val realDf = spark.read
          .format("kafka")
          .option("kafka.bootstrap.servers", "data-kafka00:8920,data-kafka01:8920,data-kafka02:8920")
          .option("assign", write(assignJson))
          .option("startingOffsets", write(startJson)) // Use the same starting and ending offsets as df's original partitions
          .option("endingOffsets", write(endJson))
          .option("minPartitions", numParts) // Split into at least numParts partitions
          .load()
        realWork(realDf)
      }
      else {} // empty batch
    }

    streamingDf.writeStream
      .foreachBatch((df: DataFrame, batchId: Long) => splitBatch(df, batchId))
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(TRIGGER_INTERVAL))
      .start()
  }

  def realWork(df: DataFrame): Unit = {
    println("realWork: ")
    // Can see that the original dataframe is split into 18 partitions in total
    for(p <- df.rdd.partitions) {
      val offset = getFields(p, "offsetRange")
      val offsetSize = getMethods(offset, "size").asInstanceOf[Long]
      println(
        s"offset = ${offset}\noffsetSize = ${offsetSize}"
      )
    }
    df.cache()
    df.show()
  }

  /**
    * Get (possibly private) fields of given object by name.
    * @param o
    * @param name
    * @return
    */
  def getFields(o: Object, name: String): Object = {
    o.getClass.getDeclaredFields
      .filter(f => f.getName == name)
      .map { f =>
        f.setAccessible(true)
        val res = f.get(o)
        f.setAccessible(false)
        res
      }
      .apply(0)
  }

  /**
    * Get (possibly private) methods of given object by name.
    * @param o
    * @param name
    * @return
    */
  def getMethods(o: Object, name: String): Object = {
    o.getClass.getDeclaredMethods
      .filter(m => m.getName == name)
      .map { m =>
        m.setAccessible(true)
        val res = m.invoke(o)
        m.setAccessible(false)
        res
      }
      .apply(0)
  }
}

/*
In spark shell:
topic = test1
partition = 0
offset = KafkaOffsetRange(test1-0,0,5752,Some(executor_hadoop-slave101.internal.grass.garenanow.com_1))
offsetSize = 5752
fromOffset = 0
untilOffset = 5752
size = 5752
numParts = 6
assignJson = Map(test1 -> List(0))
startJson=Map(test1 -> Map(0 -> 0))
endJson=Map(test1 -> Map(0 -> 5752))
topic = test2
partition = 0
offset = KafkaOffsetRange(test2-0,0,5753,Some(executor_hadoop-slave101.internal.grass.garenanow.com_1))
offsetSize = 5753
fromOffset = 0
untilOffset = 5753
size = 11505
numParts = 12
assignJson = Map(test2 -> List(0), test1 -> List(0))
startJson=Map(test2 -> Map(0 -> 0), test1 -> Map(0 -> 0))
endJson=Map(test2 -> Map(0 -> 5753), test1 -> Map(0 -> 5752))
topic = test0
partition = 0
offset = KafkaOffsetRange(test0-0,0,5753,Some(executor_hadoop-slave101.internal.grass.garenanow.com_1))
offsetSize = 5753
fromOffset = 0
untilOffset = 5753
size = 17258
numParts = 18
assignJson = Map(test2 -> List(0), test1 -> List(0), test0 -> List(0))
startJson=Map(test2 -> Map(0 -> 0), test1 -> Map(0 -> 0), test0 -> Map(0 -> 0))
endJson=Map(test2 -> Map(0 -> 5753), test1 -> Map(0 -> 5752), test0 -> Map(0 -> 5753))
realWork:
offset = KafkaOffsetRange(test1-0,0,958,None)
offsetSize = 958
offset = KafkaOffsetRange(test1-0,958,1916,None)
offsetSize = 958
offset = KafkaOffsetRange(test1-0,1916,2875,None)
offsetSize = 959
offset = KafkaOffsetRange(test1-0,2875,3834,None)
offsetSize = 959
offset = KafkaOffsetRange(test1-0,3834,4793,None)
offsetSize = 959
offset = KafkaOffsetRange(test1-0,4793,5752,None)
offsetSize = 959
offset = KafkaOffsetRange(test0-0,0,958,None)
offsetSize = 958
offset = KafkaOffsetRange(test0-0,958,1917,None)
offsetSize = 959
offset = KafkaOffsetRange(test0-0,1917,2876,None)
offsetSize = 959
offset = KafkaOffsetRange(test0-0,2876,3835,None)
offsetSize = 959
offset = KafkaOffsetRange(test0-0,3835,4794,None)
offsetSize = 959
offset = KafkaOffsetRange(test0-0,4794,5753,None)
offsetSize = 959
offset = KafkaOffsetRange(test2-0,0,958,None)
offsetSize = 958
offset = KafkaOffsetRange(test2-0,958,1917,None)
offsetSize = 959
offset = KafkaOffsetRange(test2-0,1917,2876,None)
offsetSize = 959
offset = KafkaOffsetRange(test2-0,2876,3835,None)
offsetSize = 959
offset = KafkaOffsetRange(test2-0,3835,4794,None)
offsetSize = 959
offset = KafkaOffsetRange(test2-0,4794,5753,None)
offsetSize = 959
+----+-------+-----+---------+------+--------------------+-------------+
| key|  value|topic|partition|offset|           timestamp|timestampType|
+----+-------+-----+---------+------+--------------------+-------------+
|null|   [31]|test1|        0|     0|2021-05-28 15:09:...|            0|
|null|   [34]|test1|        0|     1|2021-05-28 15:09:...|            0|
|null|   [37]|test1|        0|     2|2021-05-28 15:09:...|            0|
|null|[31 30]|test1|        0|     3|2021-05-28 15:09:...|            0|
|null|[31 33]|test1|        0|     4|2021-05-28 15:09:...|            0|
|null|[31 36]|test1|        0|     5|2021-05-28 15:09:...|            0|
|null|[31 39]|test1|        0|     6|2021-05-28 15:09:...|            0|
|null|[32 32]|test1|        0|     7|2021-05-28 15:09:...|            0|
|null|[32 35]|test1|        0|     8|2021-05-28 15:09:...|            0|
|null|[32 38]|test1|        0|     9|2021-05-28 15:09:...|            0|
|null|[33 31]|test1|        0|    10|2021-05-28 15:09:...|            0|
|null|[33 34]|test1|        0|    11|2021-05-28 15:09:...|            0|
|null|[33 37]|test1|        0|    12|2021-05-28 15:09:...|            0|
|null|[34 30]|test1|        0|    13|2021-05-28 15:09:...|            0|
|null|[34 33]|test1|        0|    14|2021-05-28 15:09:...|            0|
|null|[34 36]|test1|        0|    15|2021-05-28 15:09:...|            0|
|null|[34 39]|test1|        0|    16|2021-05-28 15:09:...|            0|
|null|[35 32]|test1|        0|    17|2021-05-28 15:09:...|            0|
|null|[35 35]|test1|        0|    18|2021-05-28 15:09:...|            0|
|null|[35 38]|test1|        0|    19|2021-05-28 15:09:...|            0|
+----+-------+-----+---------+------+--------------------+-------------+
only showing top 20 rows

 */
