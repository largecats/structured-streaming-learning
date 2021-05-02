package common.utils

import org.apache.spark.sql.catalyst.util.DateTimeConstants.MICROS_PER_DAY
import org.apache.spark.sql.catalyst.util.DateTimeUtils.toMillis
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.unsafe.types.UTF8String

object Utilities {

  /** replicate the convert function for converting time interval string to milliseconds */
  def convert(interval: String): Long = {
    val cal = IntervalUtils.stringToInterval(UTF8String.fromString(interval))
    if (cal.months != 0) {
      throw new IllegalArgumentException(s"Doesn't support month or year interval: $interval")
    }
    val microsInDays = Math.multiplyExact(cal.days, MICROS_PER_DAY)
    toMillis(Math.addExact(cal.microseconds, microsInDays))
  }

}
