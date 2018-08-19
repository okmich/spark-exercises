
import java.sql.Timestamp

case class Window(start: Timestamp, end: Timestamp)

case class WindowsCount(window_dt: Window, year:Option[Int], district: Option[Int], crime_type: String, count: Option[Long]) {
	def isin(dt: java.sql.Timestamp) : Boolean  = {
		val ts = dt.getTime
		(ts > window_dt.start.getTime) && (ts < window_dt.end.getTime)
	}
}
val ds = tWindowDF.as[WindowsCount]

val ts = new Timestamp(2018, 8, 5, 0, 0, 0, 0)
val thisweekscrimecountDF = ds.filter(_.isin(ts))






// NOT SAFE
val isin = udf(((ts: Timestamp, w: Window) => true))
ds.withColumn("a", isin(lit(ts).cast(TimestampType), $"window_dt"))
