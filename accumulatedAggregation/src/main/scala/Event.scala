import java.sql.Timestamp

/** User-defined data type representing the input events */
case class Event(uid: String, timestamp: Timestamp)