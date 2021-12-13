package org.example

import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.io.StdIn._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object sparkForNeo4j extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("gruppe343")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR 404")
  val dfContinents = spark.read.format("org.neo4j.spark.DataSource")
    .option("url", "bolt://localhost:7687")
    .option("authentication.basic.username", "neo4j")
    .option("authentication.basic.password", "student")
    .option("labels", "State")
    .load()

  val dfRelation = spark.read.format("org.neo4j.spark.DataSource")
    .option("url", "bolt://localhost:7687")
    .option("authentication.basic.username", "neo4j")
    .option("authentication.basic.password", "student")
    .option("query", "MATCH (source:State)-[r:LOCATED_IN]->(target:State) RETURN source.name, source.GdpPerCapita, target.name")
    .load()

  var stopped = false
  println("Neo4j database input opertunity")
  var command = 0


  var states = statesDf.select("name").collectAsList()

  while(!stopped){
    println(" ")
    command = readInt()
    if(command==1){
      val state = readLine("Enter the name of the state: ")
      print("Enter the GDP per Capita for this country:")
     // val income = readInt()

      println(" ")
      var c =0
      for(c <- 0 to states.size() - 1){
          println( c + " : " + states.get(c))
      }

      val cNum = readInt()
      if(cNum < 0 || cNum > 4){
        println("The number you entered dosent correspond to any of the continents.... quitting program")
        stopped = true
      }


      val data = Seq(
        (state, states.get(cNum).getString(cNum)))
      val stateDf = spark.createDataFrame(data).toDF("name", "state")

      stateDf.write
        .format("org.neo4j.spark.DataSource")
        .option("url", "bolt://localhost:7687")
        .option("authentication.basic.username", "neo4j")
        .option("authentication.basic.password", "student")
        .option("relationship", "LOCATED_IN")
        .option("relationship.save.strategy", "keys")
        .option("relationship.source.labels", ":State")
        .option("relationship.source.save.mode", "overwrite")
        .option("relationship.source.node.keys", "name:name")
        .option("relationship.source.node.properties", "GdpPerCapita:GdpPerCapita")
        .option("relationship.target.labels", ":State")
        .option("relationship.target.node.keys", "state:name")
        .option("relationship.target.save.mode", "Overwrite")
        .save()

      stateDf.show()



    }else if(command==2){
      dfRelation.show(dfRelation.count().toInt,false)

    }else if(command==3){


      println("TOP 10 (sorted by GDP per Capita)")
      val dd = spark.read.format("org.neo4j.spark.DataSource")
        .option("url", "bolt://localhost:7687")
        .option("authentication.basic.username", "neo4j")
        .option("authentication.basic.password", "student")
        .option("query", "MATCH (source:State)-[r:LOCATED_IN]->(target:State)  RETURN source.name, source.GdpPerCapita, target.name ORDER BY source.GdpPerCapita DESC")
        .load()

      dd.show(10,false)

    }

    else if(command == 4){
      stopped = true
      println("quiting program....")
    }else{
      println("you typed a wrong command, try again")

    }

  }

}

