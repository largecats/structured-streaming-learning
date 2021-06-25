import java.sql.Timestamp

/**
 * User-defined data type for storing a session information as state in flatMapGroupsWithState.
 *
 * @param users           a map from user id to first appear timestamp in milliseconds
 * @param lastTimestampMs next output starting timestamp in milliseconds
 */
case class DailyInfo(
                      users: Map[String,Long],
                      lastTimestampMs: Long)