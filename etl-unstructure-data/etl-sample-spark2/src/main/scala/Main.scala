import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

object Main {
    val fileHeaderRecordDesc = Seq(
        ("file_header", 1, 1), 
        ("file_name" , 2, 22), 
        ("file_number", 24, 3), 
        ("correction_flag", 27, 1), 
        ("file_date", 28, 6), 
        ("file_date_generated", 34, 8))

    val poolRecordDesc = Seq(
        ("cusip", 2, 9), 
        ("pool_id", 11, 6), 
        ("issue_type", 17, 1), 
        ("pool_type", 18, 2), 
        ("issue_date", 20, 8), 
        ("issuer_id", 28, 4), 
        ("as_of_date", 32, 6))

    val loanRecordDesc = Seq(
        ("pool_id", 2, 6), 
        ("seqnum", 8, 10), 
        ("issuer_id", 18, 4), 
        ("agency", 22, 1), 
        ("loan_purpose", 23, 1), 
        ("refinance_type", 24, 1), 
        ("first_payment_date", 25, 8), 
        ("maturity_date", 33, 8), 
        ("interest_rate", 41, 5), 
        ("opb_pool_issuance", 46, 11), 
        ("upb_pool_issuance", 57, 11), 
        ("upb_loan", 68, 11), 
        ("original_term", 79, 3), 
        ("loanage", 82, 3), 
        ("rem_loan_term", 85, 3), 
        ("month_delinquent", 88, 1), 
        ("month_prepaid", 89, 1), 
        ("loan_gross_margin", 90, 4), 
        ("loan_to_value", 94, 5), 
        ("combined_ltv", 99, 5), 
        ("total_debt_expense_ratio", 104, 5), 
        ("credit_score", 109, 3), 
        ("down_payment_assistance", 112, 1), 
        ("buy_down_status", 113, 1), 
        ("upfront_mip", 114, 5), 
        ("annual_mip", 119, 5), 
        ("borrower_count", 124, 1), 
        ("first_time_buyer", 125, 1), 
        ("property_type", 126, 1), 
        ("state", 127, 2), 
        ("msa", 129, 5), 
        ("third_party_origination_type", 134, 1), 
        ("curr_month_liquidation_flag", 135, 1), 
        ("removal_reason", 136, 1), 
        ("as_of_date", 137, 6), 
        ("loan_origination_date", 143, 8), 
        ("seller_issue_id", 151, 4), 
        ("index_type", 155, 5), 
        ("look_back_period", 160, 2), 
        ("interest_rate_change_date", 162, 8), 
        ("initial_int_rate_cap", 170, 1), 
        ("subsequent_int_rate_cap", 171, 1), 
        ("life_int_rate_cap", 172, 1), 
        ("nxt_int_rate_change_ceiling", 173, 5), 
        ("lifetime_int_rate_ceiling", 178, 5), 
        ("lifetime_int_rate_floor", 183, 5), 
        ("prospective_int_rate", 188, 5)
    )

    val poolActiveRecordDesc = Seq(
        ("cusip", 2, 9), 
        ("pool_id", 11, 6), 
        ("issue_type", 17, 7), 
        ("pool_type", 18, 2), 
        ("pool_issue_date", 20, 8), 
        ("issuer_id", 28, 4), 
        ("as_of_date", 32, 6), 
        ("loan_cnt", 38, 7))

    val fileTrailerRecordDesc = Seq(
        ("file_name", 2, 22), 
        ("file_no", 24, 3), 
        ("pool_cnt", 27, 7), 
        ("loan_cnt", 34, 9), 
        ("total_record_cnt", 43, 9), 
        ("as_of_date", 52, 6))

    val fileTypeMap = Map('H' -> fileHeaderRecordDesc, 
        'P' -> poolRecordDesc, 
        'L' -> loanRecordDesc, 
        'T' -> poolActiveRecordDesc, 
        'Z' -> fileTrailerRecordDesc)


    def main(args : Array[String]) : Unit = {
        if (args.length < 1){
            println(" ===> Please specify the input directory")
            System.exit(-1)
        }
        val inputDir = args(0) //the input directory

        val sparkSession = SparkSession
            .builder()
            .appName(s"SparkETLv.2")
            .enableHiveSupport()
            .getOrCreate()

        val sparkContext = sparkSession.sparkContext
        import sparkSession.implicits._
               
        //read the dataset
        val rawFileRDD = sparkContext.textFile(inputDir).
                    persist(StorageLevel.MEMORY_AND_DISK)
        
        //try to split this raw dataset into the possible five dataset that should be
        val fileHeaderRDD = rawFileRDD.filter(_.charAt(0) == 'H')
        val poolRDD = rawFileRDD.filter(_.charAt(0) == 'P')
        val loanRDD = rawFileRDD.filter(_.charAt(0) == 'L')
        val poolActiveRDD = rawFileRDD.filter(_.charAt(0) == 'T')
        val fileTrailerRDD = rawFileRDD.filter(_.charAt(0) == 'Z')

        //we will now process each of these files separately
        val fileHeaderDF = parseAndStructureTextRDD(sparkSession, fileHeaderRDD, fileHeaderRecordDesc)
        val poolDF : Dataset[PoolHeader] = parseAndStructureTextRDD(sparkSession, poolRDD, poolRecordDesc).as[PoolHeader]
        val loanDF = parseAndStructureTextRDD(sparkSession, loanRDD, loanRecordDesc)
        val poolActiveDF = parseAndStructureTextRDD(sparkSession, poolActiveRDD, poolActiveRecordDesc)
        val fileTrailerDF = parseAndStructureTextRDD(sparkSession, fileTrailerRDD, fileTrailerRecordDesc)

        //save each of them in a file table or destination
        fileHeaderDF.write.insertInto("ginnie.llmon1_file_header")
        poolDF.repartition(1).write.mode(SaveMode.Append).parquet("/apps/hive/warehouse/ginnie.db/llmon1_pool")
        loanDF.coalesce(2).write.mode(SaveMode.Overwrite).saveAsTable("ginnie.llmon1_loan")
        poolActiveDF.write.insertInto("ginnie.llmon1_active_pool_summary")
        fileTrailerDF.write.insertInto("ginnie.llmon1_file_trailer")

        //END
    }

    def parseAndStructureTextRDD(spark: SparkSession, textRDD : RDD[String], 
                struct :Seq[(String, Int, Int)]) : DataFrame = {
        val rddRow : RDD[Row] = textRDD.map((s: String) => Row.fromSeq(parseLine(s, struct)))
        val structType = generateStructType(struct)

        spark.createDataFrame(rddRow, structType)
    }

    def generateStructType(structure: Seq[(String, Int, Int)]) : StructType = {
        val dataFields = structure.map((i : (String, Int, Int)) => {
            StructField(i._1, StringType)
        })
        StructType(dataFields)
    }

    def parseLine(line: String, structure: Seq[(String, Int, Int)]) : Seq[Any] = {
        import scala.collection.mutable.ArrayBuffer
        val ret : ArrayBuffer[Any]  = new ArrayBuffer()
        for (v <- structure){
            val (beginIdx, len) = (v._2, v._3)
            val fieldValue : Any = if (line.length > len + beginIdx){
                line.substring(beginIdx -1, len + beginIdx - 1).trim
            } else null
            ret.append(fieldValue)
        }
        ret
    }

}

case class PoolHeader(cusip: String, pool_id: String, issue_type: String, pool_type: String,
        issue_date: String, issuer_id: String, as_of_date: String) extends java.io.Serializable