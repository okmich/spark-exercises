import java.io.{PrintWriter, FileWriter}
import org.apache.spark.sql.DataFrame

def saveSchema(file: String, df: DataFrame) = {
	val ps = new PrintWriter(new FileWriter(file))
	ps.println(df.schema.json)
	ps.close
}


val testItemDF = spark.read.options(Map("sep" -> "|", "header" -> "true", "inferSchema" -> "true")).csv("/user/maria_dev/mot_data/test_item")

val testResultDF = spark.read.options(Map("sep" -> "|", "header" -> "true", "inferSchema" -> "true")).csv("/user/maria_dev/mot_data/test_result")

val testDetailsDF = spark.read.options(Map("sep" -> "|", "header" -> "true", "inferSchema" -> "true")).csv("/user/maria_dev/mot_data/item_details")

saveSchema("testitem.json", testItemDF)
saveSchema("testresult.json", testResultDF)
saveSchema("testdetail.json", testDetailsDF)
