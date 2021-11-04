import java.io._
import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import org.mongodb.scala._
import org.mongodb.scala.model._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Updates._
import org.mongodb.scala.model.UpdateOptions
import org.mongodb.scala.bson.BsonObjectId


object MongoGetPostTest extends App {
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
    val collection: MongoCollection[Document] = database.getCollection("arrests_per_100k");

    // val document = Document(
    //     "states" -> "Delaware", 
    //     "arrests" -> 32,
    //     "contact" -> Document("Discord" -> "Mariusge#5190", 
    //                             "email" -> "mariusge@hiof.no"),
    //     "beard_state" -> "Excellent", 
    //     "taught_autumn_2021" -> Seq(
    //         "Masterstudium i Organisasjon og ledelse - MOL 2",
    //         "Big Data - Lagring og bearbeiding"
    //     ));


    // Document objektet for mongo
    val arrests_per_100k = Document (
        "states" -> row(1),
        "total_arrests" -> Document (
            "murder_arrests_per_100k" -> 5.9,
            "assault_arrests_per_100k" -> 4.9,
            "rape_arrests_per_100k" -> 3.4
        )
    );

    collection.insertOne(arrests_per_100k).results();
}