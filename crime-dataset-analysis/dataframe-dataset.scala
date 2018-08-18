//dataframe vs dataset
//https://databricks.com/blog/2016/07/14/a-tale-of-three-apache-spark-apis-rdds-dataframes-and-datasets.html
//https://databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html
//https://databricks.com/blog/2015/04/28/project-tungsten-bringing-spark-closer-to-bare-metal.html

//A Dataset is a strongly-typed, immutable collection of objects that are mapped to a relational schema. 
//with every dataset is an encoder that is responsible to converting between the relational representation and the jvm objects

//datasets are also IDE-friendly, compile-type safe

//crimeDF is a dataframe

//let create a dataset
case class Crime(id: Int, case_no: String, date: String, block: String, crime_type: String, description: String, 
	location_desc: String, arrest: Boolean, domestic: Boolean, district: Int, ward: Int, community: Int, 
	update_dt: String, year: Int, longitude: Double, latitude: Double) extends java.io.Serializable {

	def ledToArrest : Boolean = this.arrest
}

val ds = crimeDF.as[Crime]

//using dataset with lambdas
val arrestedCrimeDs = ds.filter(_.ledToArrest) // same as crimeDF.where($"arrest" === true)