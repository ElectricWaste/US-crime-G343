// import here


case class NodeResponse(key : String, value : String)
case class Node(key : String, value : String, nodes: Array[NodeResponse])
case class Message(action: String, node: Node)


object HttpGetPostTest extends App {

    val url = ""
    val result = scala.io.Source.formURL(url).mkString

    println(result);

    val message_Parsed = new Gson().fromJson(result, classOf[Message]);
    val front_Page_Parsed = new Gson().fromJson(message_Parsed.node.nodes(0).value , classOf[Violent_Crime_By_State]);
    val front_Page_Parsed_2 = new Gson().fromJson(message_Parsed.node.nodes(0).value , classOf[Violent_Crime_By_Crime]);

    println(front_Page_Parsed);


    for(i <- front_Page_Parsed){
        println("state: " + front_Page_Parsed.state.toString);
    }
    for(i <- front_Page_Parsed_2) {
        println(front_Page_Parsed_2.toString);
    }
}
