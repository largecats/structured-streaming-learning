import common.utils.Utilities.convert

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.{Encoder, Encoders}
import java.sql.Timestamp

object Main {
  /**
   * trigger_interval    interval of how often microBatch is triggered
   * partition_interval  interval of how frequently to reset the count
   * accumulate_interval interval of how frequently to generate a new count
   * timeout_duration    after how long the state shall timeout when no new data of this group appears
   */
  val trigger_interval = "30 seconds"
  val partition_interval = "10 minutes" // In the actual dau case, this is 1 day
  val accumulate_interval = "1 minutes" // in the actual dau case, this is 5 minutes
  val timeout_duration = "1 minutes"

  def main(args: Array[String]): Unit = {
    lazy val spark = SparkSession.builder().master("local[4]").getOrCreate()
    /**
     * state files will be created for each partition
     * adjust this can avoid creating states for empty partitions
     */
    spark.conf.set("spark.sql.shuffle.partitions",1)

    import spark.implicits._

    /*
    Rate source (for testing): Generates data at the specified number of rows per second, each output row contains
    a timestamp and value:
      - timestamp is a Timestamp type containing the time of message dispatch,
      - value is of Long type containing the message count, starting from 0 as the first row.
     */
    val ds = spark.
      readStream.
      format("rate").
      option("numPartitions", 2).
      option("rowsPerSecond", 1).
      option("rampUpTime", 3).
      load()

    /** transform the rate stream into Event stream */
    // The Event case class must be defined outside the object scope: https://stackoverflow.com/questions/38664972/why-is-unable-to-find-encoder-for-type-stored-in-a-dataset-when-creating-a-dat
    val events = ds.as[(Timestamp,Long)].map{ case(timestamp,value) => Event(uid=value.toString, timestamp) }

    /** groupByKey the partition_interval starting timestamp in milliseconds */
    val dailyAppend = events.
      groupByKey(event => event.timestamp.getTime/convert(partition_interval)*convert(partition_interval)).
      flatMapGroupsWithState(OutputMode.Append,GroupStateTimeout.ProcessingTimeTimeout)(accumulatedDistinctCount)

    /*
    Console sink (for debugging) - Prints the output to the console/stdout every time there is a trigger.
    The entire output is collected and stored in the driver’s memory after every trigger.
     */
    val query = dailyAppend.
      writeStream.
      format("console").
      option("numRows",10).
      option("truncate",false).
      outputMode("append").
      trigger(Trigger.ProcessingTime(trigger_interval)).
      start()

    query.awaitTermination()
  }

  /**
   * Function for flatMapGroupsWithState
   *
   * since the very last part of each batch group data might not fully serve one accumulate_interval,
   * have to calculate up to which timestamp in milliseconds can output result.
   *
   * there's no signal when will the very last accumulate_interval in each partition_interval is complete,
   * so instead, only output result when the state timeout for the last accumulate_interval.
   * note that depending on the settings of trigger_interval, accumulate_interval and timeout_duration, this last accumulate_interval result might appear after the first few accumulate_interval results of the next partition_interval.
   */
  def accumulatedDistinctCount(partition_key: Long, events: Iterator[Event], state: GroupState[DailyInfo]): Iterator[DailyAppend] = {
    if (state.hasTimedOut) {
      val stateToRemove = state.get
      val allUsers = stateToRemove.users
      val lastTimestampMs = stateToRemove.lastTimestampMs
      state.remove()
      /** output the very last accumulate_interval of each partition_interval */
      /*
      Create time range:
      - start from lastTimestampMs
      - end (inclusive) at partition_key + partition_interval - accumulate_interval
      - step by accumulate_interval
      For each timestamp ms in the above range, create DailyAppend(ms, user count since the start of partition_interval).
      Return an iterator of these DailyAppend objects. This goes into the output stream.
       */
      (lastTimestampMs to (partition_key+convert(partition_interval)-convert(accumulate_interval)) by convert(accumulate_interval)).map(ms => DailyAppend(new Timestamp(ms),allUsers.values.filter(_<ms+convert(accumulate_interval)).size)).iterator
    } else {
      // Get the earliest time each user appeared
      val newUsers = events.toSeq.groupBy(_.uid).mapValues(_.map{e => e.timestamp.getTime}.min)
      // Get the latest time of appearance among all new users
      val calNewLastTimestampMs = (newUsers.values.max-1)/convert(accumulate_interval)*convert(accumulate_interval)
      // Set newLastTimestampMs (used in updating the state) to max(latest time of appearance among all new users, partition_key)
      val newLastTimestampMs = if (calNewLastTimestampMs < partition_key) {
        partition_key
      } else {
        calNewLastTimestampMs
      }
      val (allUsers, lastTimestampMs) = if (state.exists) {
        val oldDaily = state.get
        (newUsers ++ oldDaily.users, oldDaily.lastTimestampMs)
      } else {
        (newUsers, partition_key)
      }
      val updateDailyInfo = DailyInfo(allUsers,newLastTimestampMs)
      state.update(updateDailyInfo)
      state.setTimeoutDuration(timeout_duration)
      /*
      Create time range:
      - start at lastTimestampMs
      - end (exclusive) at newLastTimestampMs
      - step by accumulate_interval
      For each timestamp ms in the above range, create DailyAppend(ms, user count since the start of partition_interval).
       */
      (lastTimestampMs until newLastTimestampMs by convert(accumulate_interval)).map(ms => DailyAppend(new Timestamp(ms),allUsers.values.filter(_<ms+convert(accumulate_interval)).size)).iterator
    }
  }
}

/*
Batch 0 at time 0
-------------------------------------------
Batch: 0
-------------------------------------------
+----+-------------------+
|time|accumulatedDistinct|
+----+-------------------+
+----+-------------------+

Batch 1 at time 30s
-------------------------------------------
Batch: 1
-------------------------------------------
+----+-------------------+
|time|accumulatedDistinct|
+----+-------------------+
+----+-------------------+

Batch 2 at time 1min (Why only 15:17 has a non-zero count?
-------------------------------------------
Batch: 2
-------------------------------------------
+-------------------+-------------------+
|time               |accumulatedDistinct|
+-------------------+-------------------+
|2021-05-02 15:10:00|0                  |
|2021-05-02 15:11:00|0                  |
|2021-05-02 15:12:00|0                  |
|2021-05-02 15:13:00|0                  |
|2021-05-02 15:14:00|0                  |
|2021-05-02 15:15:00|0                  |
|2021-05-02 15:16:00|0                  |
|2021-05-02 15:17:00|1                  |
+-------------------+-------------------+

Batch 3 at time 1min30s
-------------------------------------------
Batch: 3
-------------------------------------------
+----+-------------------+
|time|accumulatedDistinct|
+----+-------------------+
+----+-------------------+

Batch 4 at time 2min
-------------------------------------------
Batch: 4
-------------------------------------------
+-------------------+-------------------+
|time               |accumulatedDistinct|
+-------------------+-------------------+
|2021-05-02 15:18:00|60                 |
+-------------------+-------------------+

Batch 5 at time 2min30s
-------------------------------------------
Batch: 5
-------------------------------------------
+----+-------------------+
|time|accumulatedDistinct|
+----+-------------------+
+----+-------------------+

Batch 6 at time 3min
-------------------------------------------
Batch: 6
-------------------------------------------
+----+-------------------+
|time|accumulatedDistinct|
+----+-------------------+
+----+-------------------+

Batch 7 at time 3min30s
-------------------------------------------
Batch: 7
-------------------------------------------
+----+-------------------+
|time|accumulatedDistinct|
+----+-------------------+
+----+-------------------+

Batch 8 at time 4min
-------------------------------------------
Batch: 8
-------------------------------------------
+-------------------+-------------------+
|time               |accumulatedDistinct|
+-------------------+-------------------+
|2021-05-02 15:20:00|60                 |
+-------------------+-------------------+

Batch 9 at time 4min30s
-------------------------------------------
Batch: 9
-------------------------------------------
+-------------------+-------------------+
|time               |accumulatedDistinct|
+-------------------+-------------------+
|2021-05-02 15:19:00|120                |
+-------------------+-------------------+

Batch 10 at time 5min
-------------------------------------------
Batch: 10
-------------------------------------------
+-------------------+-------------------+
|time               |accumulatedDistinct|
+-------------------+-------------------+
|2021-05-02 15:21:00|120                |
+-------------------+-------------------+

 */