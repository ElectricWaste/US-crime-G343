import java.io._
import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import org.mongodb.scala._
import org.mongodb.scala.model._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Projections._
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala.model.Updates._
import org.mongodb.scala.model.UpdateOptions
import org.mongodb.scala.bson.BsonObjectId


object insertMongoObj extends App {
    implicit class DocumentObservable[C](val observable: Observable[Document]) extends ImplicitObservable[Document] {
        override val converter: (Document) => String = (doc) => doc.toJson
    }

    implicit class GenericObservable[C](val observable: Observable[C]) extends ImplicitObservable[C] {
        override val converter: (C) => String = (doc) => doc.toString
    }

    trait ImplicitObservable[C] {
        val observable: Observable[C]
        val converter: (C) => String

        def results(): Seq[C] = Await.result(observable.toFuture(), Duration(10, TimeUnit.SECONDS))
        def headResult() = Await.result(observable.head(), Duration(10, TimeUnit.SECONDS))
        def printResults(initial: String = ""): Unit = {
        if (initial.length > 0) print(initial)
        results().foreach(res => println(converter(res)))
        }
        def printHeadResult(initial: String = ""): Unit = println(s"${initial}${converter(headResult())}")
    }

    val mongoClient: MongoClient = MongoClient();
    val database: MongoDatabase = mongoClient.getDatabase("uscrime");
    val collection: MongoCollection[Document] = database.getCollection("arrest_per_100k");

    val source = io.Source.fromFile("US_violent_crime.csv")


    source.getLines.drop(1).foreach { line =>
        val row = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)").map(_.trim)
        println(row.mkString(","))

        def parseDouble(value : String) : Option[Double] = if (value == "") None else Some(value.toDouble)

    println("Success")

        val document = Document(
            "State" -> row(0),
            "Murder" -> parseDouble(row(1)),
            "Assault" -> parseDouble(row(2)),
            "Urbanpop" -> parseDouble(row(3)),
            "Rape" -> parseDouble(row(4))
            );

        collection.insertOne(document).results();
    }

    println("All documents inserted")
}