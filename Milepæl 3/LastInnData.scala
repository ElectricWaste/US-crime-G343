// import here



case class Violent_Crime_By_State(state: String)
case class Violent_Crime_By_Crime(crime: Array[String])

object Violent_Crime_By_State extends App {

    val source = io.Source.fromFile("US_violent_crime.csv")
    var lines = source.getLines()


}

object Violent_Crime_By_Crime extends App {

    val source = io.Source.fromFile("US_violent_crime.csv")
    var lines = source.getLines()

}
