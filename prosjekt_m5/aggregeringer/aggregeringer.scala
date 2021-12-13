import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object aggregeringer extends App {

    val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("gruppe343")
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR 404")

    val dataframe = spark.read.format("parquet").load("")   

    
    val  = dataframe.sort(col("").desc).limit(10).write.format("csv").option("header", true).save("parquet/")
    

    // Shootings done by police
    val shootingsByPolice = dataframe.sort(col("").desc).limit(10).write.format("csv").option("header", true).save("aggCsvFiles/shootings.csv")

    // US violent crime rates
    val violentCrime = dataframe.sort(col("").desc).limit(10).write.format("csv").option("header", true).save("aggCsvFiles/US_violent_crime.csv")


}
