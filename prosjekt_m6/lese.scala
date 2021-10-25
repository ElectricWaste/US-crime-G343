import java.io._
import org.apache.commons._

import org.apache.http.{io => _,_  } 
import org.apache.http.client._
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.DefaultHttpClient
import java.util.ArrayList
import org.apache.http.message.BasicNameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import com.google.gson.Gson

case class NodeResponse(key : String, value: String)
case class Node(key : String, value: String, nodes: Array[NodeResponse])
case class Message(action: String, node: Node)

case class stateData( murder: Double, assault: Double, urbanpop: Double, rape: Double  )

object HttpGetPostTest extends App {

    def retrieveKeyValue (key: String): Unit = {
        val url = "http://127.0.0.1:2379/v2/keys/" + key
        val result = scala.io.Source.fromURL(url).mkString
        
        println(result);
        
        val messageParsed = new Gson().fromJson( result, classOf[Message] );
        val valueParsed = new Gson().fromJson( messageParsed.node.nodes(0).value, classOf[stateData] );
        
        println(valueParsed);
        println("State: " + key);
        println("Murder: " + valueParsed.murder);
        println("Assault: " + valueParsed.assault);
        println("Urban Pop: " + valueParsed.urbanpop);
        println("Rape: " + valueParsed.rape);
    }

    val file = "US_violent_crime.csv"
    val source = io.Source.fromFile(file)
    //// Lesing og pushing av key values i KV store
    source.getLines.drop(1).foreach {line =>
        val row = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)").map(_.trim)
        // Sletting av gåseøyne
        val country_0 = row(0).toString.replaceAll("\"", "")
        val country = country_0.toString.replaceAll(" ", "_")
        
        println(country);
        

        println("Inserting{ state: " + country + ", murder: " + row(1).toDouble + ", assault: " + row(2).toDouble + ", urbanPop: " + row(3).toDouble + ", rape: " + row(4).toDouble )

        val stateObject = new stateData(row(1).toDouble, row(2).toDouble, row(3).toDouble, row(4).toDouble)
        // val key = row(0).toString


        val stateObject_asJson = new Gson().toJson(stateObject)
        println(stateObject_asJson)

        val url = "http://127.0.0.1:2379/v2/keys/" + country
        
        
        // add name value pairs to a post object
        
        val post = new HttpPost(url)
        
        val nameValuePairs = new ArrayList[NameValuePair]()
        nameValuePairs.add(new BasicNameValuePair("value", stateObject_asJson))
        post.setEntity(new UrlEncodedFormEntity(nameValuePairs))

        // send the post request
        val client = new DefaultHttpClient
        val response = client.execute(post)
        println("--- HEADERS ---")
        response.getAllHeaders.foreach(arg => println(arg))


    
    }

// Eksempel på uthenting
    retrieveKeyValue("New_Hampshire")
    retrieveKeyValue("Nebraska")


}