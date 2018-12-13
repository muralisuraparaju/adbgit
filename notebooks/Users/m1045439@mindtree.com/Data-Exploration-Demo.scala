// Databricks notebook source
//DEFINE THE INPUT FILES
val businessFile = "dbfs:/mnt/dataingestiondemo/data/input/businesses_plus.csv"
val inspectionFile = "dbfs:/mnt/dataingestiondemo/data/input/inspections_plus.csv"
val violationFile = "dbfs:/mnt/dataingestiondemo/data/input/violations_plus.csv"

// COMMAND ----------

//LOAD THE DATA TO DATAFRAMES AND CREATE TEMP TABLES FOR EXPLORATION
val businessDF = spark.read.format("csv").option("header", "true").option("delimiter", ",").load(businessFile)
val inspectionDF = spark.read.format("csv").option("header", "true").option("delimiter", ",").load(inspectionFile)
val violationDF = spark.read.format("csv").option("header", "true").option("delimiter", ",").load(violationFile)

businessDF.createOrReplaceTempView("business")
inspectionDF.createOrReplaceTempView("inspection")
violationDF.createOrReplaceTempView("violation")

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Do some basic exploration of data
// MAGIC SELECT DISTINCT(city) FROM business
// MAGIC 
// MAGIC --Data quality issues !!

// COMMAND ----------

//Lets clean up the cities a bit

//Create a User defined function
def cleanUpCity=(s: String) => {
val cityMap = Map(
"S.F."->"SFO",
"sf"->"SFO",
"s.F. Ca"->"SFO",
"SF`"->"SFO",
"SO. SAN FRANCISCO"->"SFO",
"San Franciscvo"->"SFO",
"san Francisco"->"SFO",
"San Francisco"->"SFO",
"null"->"SFO",
"San Francsico"->"SFO",
"Sf"->"SFO",
"SO.S.F."->"SFO",
"San Francisco,"->"SFO",
"CA"->"SFO",
"san Francisco CA"->"SFO",
"San Francisco, CA"->"SFO",
"SF"->"SFO",
"SF, CA"->"SFO",
"SF CA  94133"->"SFO",
"San francisco"->"SFO",
"SF , CA"->"SFO",
"San Francisco, Ca"->"SFO",
"SAN FRANCICSO"->"SFO",
"San Franciisco"->"SFO",
"Sand Francisco"->"SFO",
"SF."->"SFO",
"SAN FRANCISCO"->"SFO",
"san francisco"->"SFO",
"San Franicisco"->"SFO",
"S F"->"SFO",
"OAKLAND" -> "OAKLAND", 
"Oakland" -> "OAKLAND")
  
  if(cityMap.get(s).isDefined) {
    cityMap.get(s).get
  } else{
    s
  }
}

//register the UDF
sqlContext.udf.register("clean_city",cleanUpCity)

//clean up the data
val cleanedBusinessDF = spark.sql("""
SELECT business_id,
name,
address,
clean_city(city) city,
postal_code,
latitude,
longitude,
phone_number,
TaxCode,
business_certificate,
application_date,
owner_name,
owner_address,
owner_city,
owner_state,
owner_zip
FROM business
""")
cleanedBusinessDF.createOrReplaceTempView("business")
display(cleanedBusinessDF)

// COMMAND ----------

// MAGIC %sql
// MAGIC -- DO ADVANCED EXPLORATION IN SQL
// MAGIC -- QUESTION: FIND AVERAGE SCORE OF EACH BUSINESS
// MAGIC 
// MAGIC SELECT a.name AS Business, AVG(b.Score) AS AVG_SCORE FROM business a, inspection b 
// MAGIC WHERE a.business_id=b.business_id 
// MAGIC AND b.Score IS NOT NULL
// MAGIC GROUP BY a.name
// MAGIC ORDER BY AVG_SCORE ASC

// COMMAND ----------

// MAGIC %sql
// MAGIC -- MORE EXPLORATION: 
// MAGIC --QUESTION: FIND CITIES THAT HAVE HIGHEST VIOLATIONS
// MAGIC 
// MAGIC SELECT a.city, count(*) AS num_violations FROM business a, violation b WHERE a.business_id=b.business_id 
// MAGIC GROUP BY a.city
// MAGIC ORDER BY num_violations DESC
// MAGIC 
// MAGIC -- NOTE: Data quality issues. Multiple values for San Fransisco and null values of city

// COMMAND ----------

//Cubing queries
val businessScoresByYear = spark.sql("""
SELECT a.name, substring(b.inspections_date, 1,4) AS inspections_year, b.Score from 
business a, inspection b
WHERE a.business_id=b.business_id
AND b.Score IS NOT NULL
""")
businessScoresByYear.createOrReplaceTempView("business_scores_by_year")

val averageScoresDF = spark.sql("""
 SELECT name, inspections_year, avg(Score) AS AVG_SCORE FROM 
business_scores_by_year
GROUP BY name, inspections_year
ORDER BY name, inspections_year
 """)

import org.apache.spark.sql.functions._
val cubedDF = averageScoresDF.cube("name", "inspections_year").agg(avg("AVG_SCORE").alias("YEARLY_AVG"))
   .sort($"name".desc_nulls_last, $"inspections_year".asc_nulls_last)
display(cubedDF)