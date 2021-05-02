import java.sql.Timestamp

/**
 * User-defined data type representing the append information returned by flatMapGroupsWithState.
 *
 * @param time                timestamp of the starting point of the accumulate_interval window
 * @param accumulatedDistinct accumulated distinct count since the beginning of the partition_interval
 */
case class DailyAppend(
                        time: Timestamp,
                        accumulatedDistinct: Long)