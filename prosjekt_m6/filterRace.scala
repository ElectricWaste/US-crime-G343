import org.apache.spark.sql.SparkSession

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object filterForRace {

    def main(args: Array[String]){

    val chosenFile = "shootings.csv"

    val spark = SparkSession.builder
        .config("spark.master", "local")
        .appName("Filter race").getOrCreate()


    val shootingsDf = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load(chosenFile)

    val cacheDf = shootingsDf.cache()


    //Filtrering av skytinger etter raser
    val whiteShootings = cacheDf.filter(col("race") === "White").count()
    val blackShootings = cacheDf.filter(col("race") === "Black").count()
    val asianShootings = cacheDf.filter(col("race") === "Asian").count()
    val hispanicShootings = cacheDf.filter(col("race") === "Hispanic").count()


    //Printer ut til konsoll antalle skytinger i hver "rase"s
    println(
        "Antalle skytinger filtrert etter rase er:" +
        s" Hvite: $whiteShootings" +
        s" Sorte: $blackShootings" +
        s" Asiatisk: $asianShootings" +
        s" Latinamerikaner: $hispanicShootings"
    )


   // println(s" Antalle skytinger, hvite: $whiteShootings, sorte: $blackShootings, asiatisk: $asianShootings, latino: $hispanicShootings")

    spark.stop()

    }
}
