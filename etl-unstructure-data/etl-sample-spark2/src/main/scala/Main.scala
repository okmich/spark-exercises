import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

object Main {

    val ERROR_KEYWRD = "Error###"

    val fileHeaderRecordDesc = Seq(
        ("file_header", 1, 1, "string"), 
        ("file_name" , 2, 22, "string"), 
        ("file_number", 24, 3, "string"), 
        ("correction_flag", 27, 1, "string"), 
        ("file_date", 28, 6, "date|yyyyMM"), 
        ("file_date_generated", 34, 8, "date|yyyyMMdd"))

    val poolRecordDesc = Seq(
        ("cusip", 2, 9, "string"), 
        ("pool_id", 11, 6, "string"), 
        ("issue_type", 17, 1, "string"), 
        ("pool_type", 18, 2, "string"), 
        ("issue_date", 20, 8, "date|yyyyMMdd"), 
        ("issuer_id", 28, 4, "string"), 
        ("as_of_date", 32, 6, "date|yyyyMM"))

    val loanRecordDesc = Seq(
        ("pool_id", 2, 6, "string"), 
        ("seqnum", 8, 10, "string"), 
        ("issuer_id", 18, 4, "string"), 
        ("agency", 22, 1, "string"), 
        ("loan_purpose", 23, 1, "string"), 
        ("refinance_type", 24, 1, "string"), 
        ("first_payment_date", 25, 8, "date|yyyyMMdd"), 
        ("maturity_date", 33, 8, "date|yyyyMMdd"), 
        ("interest_rate", 41, 5, "decimal|2,3"), 
        ("opb_pool_issuance", 46, 11, "decimal|9,2"), 
        ("upb_pool_issuance", 57, 11, "decimal|9,2"), 
        ("upb_loan", 68, 11, "decimal|9,2"), 
        ("original_term", 79, 3, "smallint"), 
        ("loanage", 82, 3, "smallint"), 
        ("rem_loan_term", 85, 3, "smallint"), 
        ("month_delinquent", 88, 1, "smallint"), 
        ("month_prepaid", 89, 1, "smallint"), 
        ("loan_gross_margin", 90, 4, "decimal|1,3"), 
        ("loan_to_value", 94, 5, "decimal|3,2"), 
        ("combined_ltv", 99, 5, "decimal|3,2"), 
        ("total_debt_expense_ratio", 104, 5, "decimal|3,2"), 
        ("credit_score", 109, 3, "smallint"), 
        ("down_payment_assistance", 112, 1, "string"), 
        ("buy_down_status", 113, 1, "string"), 
        ("upfront_mip", 114, 5, "decimal|2,3"), 
        ("annual_mip", 119, 5, "decimal|2,3"), 
        ("borrower_count", 124, 1, "smallint"), 
        ("first_time_buyer", 125, 1, "string"), 
        ("property_type", 126, 1, "string"), 
        ("state", 127, 2, "string"), 
        ("msa", 129, 5, "int"), 
        ("third_party_origination_type", 134, 1, "string"), 
        ("curr_month_liquidation_flag", 135, 1, "string"), 
        ("removal_reason", 136, 1, "string"), 
        ("as_of_date", 137, 6, "date|yyyyMM"), 
        ("loan_origination_date", 143, 8, "date|yyyyMMdd"), 
        ("seller_issue_id", 151, 4, "string"), 
        ("index_type", 155, 5, "string"), 
        ("look_back_period", 160, 2, "smallint"), 
        ("interest_rate_change_date", 162, 8, "date|yyyyMMdd"), 
        ("initial_int_rate_cap", 170, 1, "smallint"), 
        ("subsequent_int_rate_cap", 171, 1, "smallint"), 
        ("life_int_rate_cap", 172, 1, "smallint"), 
        ("nxt_int_rate_change_ceiling", 173, 5, "decimal|2,3"), 
        ("lifetime_int_rate_ceiling", 178, 5, "decimal|2,3"), 
        ("lifetime_int_rate_floor", 183, 5, "decimal|2,3"), 
        ("prospective_int_rate", 188, 5, "decimal|2,3")
    )

    val poolActiveRecordDesc = Seq(
        ("cusip", 2, 9, "string"), 
        ("pool_id", 11, 6, "string"), 
        ("issue_type", 17, 7, "string"), 
        ("pool_type", 18, 2, "string"), 
        ("pool_issue_date", 20, 8, "date|yyyyMMdd"), 
        ("issuer_id", 28, 4, "string"), 
        ("as_of_date", 32, 6, "date|yyyyMM"), 
        ("loan_cnt", 38, 7, "smallint"))

    val fileTrailerRecordDesc = Seq(
        ("file_name", 2, 22, "string"), 
        ("file_no", 24, 3, "string"), 
        ("pool_cnt", 27, 7, "int"), 
        ("loan_cnt", 34, 9, "int"), 
        ("total_record_cnt", 43, 9, "bigint"), 
        ("as_of_date", 52, 6, "date|yyyyMM"))

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
        //generating good and bad data
        val (fHeaderValidDF, fHeaderInValidDF) =  generateValidAndInvalidDF(
                                                            fileHeaderRDD, fileHeaderRecordDesc, sparkSession)
        val (poolValidDF, poolInvalidDF) = generateValidAndInvalidDF(poolRDD, poolRecordDesc, sparkSession)
        val (loanValidDF, loanInvalidDF) = generateValidAndInvalidDF(loanRDD, loanRecordDesc, sparkSession)
        val (poolActiveValidDF, poolActiveInvalidDF) = generateValidAndInvalidDF(
                                                    poolActiveRDD, poolActiveRecordDesc, sparkSession)
        val (fTrailerValidDF, fTrailerInValidDF) =  generateValidAndInvalidDF(
                                                            fileTrailerRDD, fileTrailerRecordDesc, sparkSession)

        //Decide what to do with the bad data.
        //either store them to ship them to some other repository and inform support
        //TODO

        //save each of the good data to hive tables
        fHeaderValidDF.write.insertInto("ginnie.llmon1_file_header")
        poolValidDF.repartition(1).write.mode(SaveMode.Append).parquet("/apps/hive/warehouse/ginnie.db/llmon1_pool")
        loanValidDF.coalesce(2).write.mode(SaveMode.Overwrite).saveAsTable("ginnie.llmon1_loan")
        poolActiveValidDF.write.insertInto("ginnie.llmon1_active_pool_summary")
        fTrailerValidDF.write.insertInto("ginnie.llmon1_file_trailer")
        
        //END
    }

    def generateValidAndInvalidDF(stringRDD: RDD[String], struct :Seq[(String, Int, Int, String)],
                    spark: SparkSession) : (DataFrame, DataFrame) = {
        val rowRDD =  parseAndStructureTextToRowRDD(stringRDD, struct)
        val validDF = getDF(spark, rowRDD, struct, true)
        val invalidDF = getDF(spark, rowRDD, struct, false)

        (validDF, invalidDF)
    }

    def parseAndStructureTextToRowRDD(textRDD : RDD[String], 
            struct :Seq[(String, Int, Int, String)]) = {
        textRDD.map((s: String) => Row.fromSeq(parseLine(s, struct)))
    }

    def getDF(spark: SparkSession, rowRDD: RDD[Row], 
            struct :Seq[(String, Int, Int, String)], isValid: Boolean) : DataFrame ={
        val structType = generateStructType(struct, isValid)
        val rddRow = rowRDD.filter((row: Row) => {
                    val fields = row.toSeq
                    if (isValid)
                        fields.forall((v:Any) => v == null || !v.toString.equals(ERROR_KEYWRD))
                    else 
                        fields.forall((v:Any) => v != null && v.toString.equals(ERROR_KEYWRD))
                })
        spark.createDataFrame(rddRow, structType)
    }

    def generateStructType(structure: Seq[(String, Int, Int, String)], isValid: Boolean) : StructType = {
        val dataFields = structure.map((i : (String, Int, Int, String)) => {
            val dataType : DataType = i._4.split("\\|")(0).toLowerCase match {
                case "string" => StringType
                case "bigint" => LongType
                case "int" => IntegerType
                case "date" => DateType
                case "smallint" => ShortType
                case "decimal" => {
                    val parts = i._4.split("\\|")(1).split(",")
                    new DecimalType(parts(0).toInt + parts(1).toInt ,parts(1).toInt)
                }
                case _ => throw new IllegalArgumentException("unsupported type")
            }
            StructField(i._1, if (isValid) dataType else StringType)
        })
        StructType(dataFields)
    }

    def parseLine(line: String, structure: Seq[(String, Int, Int, String)]) : Seq[Any] = {
        import scala.collection.mutable.ArrayBuffer
        val ret : ArrayBuffer[Any]  = new ArrayBuffer()
        for (v <- structure){
            val (beginIdx, len) = (v._2, v._3)
            val fieldValue : Any = if (line.length >= (len + beginIdx - 1)){
                line.substring(beginIdx -1, len + beginIdx - 1).trim
            } else null
            val conformedType = conformToDataType(fieldValue, v._4)
            ret.append(conformedType)
        }
        ret
    }

    def conformToDataType(fieldVal: Any, dataTypeStr: String): Any = {
        if (fieldVal == null) null
        else
            try {
                dataTypeStr.split("\\|")(0).toLowerCase match {
                    case "string" => fieldVal.toString
                    case "smallint" =>  if (fieldVal.toString.trim.isEmpty) null else fieldVal.toString.toShort
                    case "bigint" => if (fieldVal.toString.trim.isEmpty) null else fieldVal.toString.toLong
                    case "int" => if (fieldVal.toString.trim.isEmpty) null else fieldVal.toString.toInt
                    case "date" => {
                        if (fieldVal.toString.trim.isEmpty) null else {
                            val format = dataTypeStr.split("\\|")(1)
                            new java.sql.Date(new java.text.SimpleDateFormat(format).parse(fieldVal.toString).getTime)
                        }
                    }
                    case "decimal" => {
                        if (fieldVal.toString.trim.isEmpty) null
                        else {
                            val format = dataTypeStr.split("\\|")(1).split(",")
                            val str = fieldVal.toString
                            val x = format(0).toInt
                            //x,y
                            new java.math.BigDecimal(str.substring(0,x) + "." + str.substring(x))
                        }
                    }
                    case _ => throw new IllegalArgumentException("unsupported type")
                }
            } catch {
                case _ : Throwable => ERROR_KEYWRD
            }
    }

}

case class PoolHeader(cusip: String, pool_id: String, issue_type: String, pool_type: String,
        issue_date: String, issuer_id: String, as_of_date: String) extends java.io.Serializable