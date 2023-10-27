import scala.sys.process._

object Extract {
//Extraction of games pages to ./games on masternode!
  def json_extraction():Int={
    var outp: Int = 1
    try {
      var r1 = requests.get("https://www.balldontlie.io/api/v1/games?seasons[]=2021")
      var json1 = ujson.read(r1.text)
      val gameP = json1("meta")("total_pages").num.toInt
      for( i <- 1 to gameP){
        var gameURL = s"https://www.balldontlie.io/api/v1/games?page=${i}&seasons[]=2021"
        println(gameURL)
        r1 = requests.get(gameURL)
        if (r1.statusCode == 200){
          json1 = ujson.read(r1.text)
          var fln = s"games_${i}.json"
          os.write(os.pwd/"games"/ fln, json1("data"))
          //save to hdfs does not work
          //os.write(os.Path("hdfs://172.31.9.8:9000/scala")/ fln, json1("data"))
          println(i)
        }else
          print("probleme with games extraction")
        Thread.sleep(1000)
      }

      //Extraction of stats pages to ./stats on masternode!
      var r4 = requests.get("https://www.balldontlie.io/api/v1/stats?seasons[]=2021")
      var json4 = ujson.read(r4.text)
      val statsP = json4("meta")("total_pages").num.toInt
      //for( i <- 1 to statsP){
      for( i <- 1 to 200){
        println(i)
        var statsURL = s"https://www.balldontlie.io/api/v1/stats?page=${i}&seasons[]=2021"
        println(statsURL)
        r4 = requests.get(statsURL)
        if (r4.statusCode == 200){
          json4 = ujson.read(r4.text)
          var fln = s"stats_${i}.json"
          os.write(os.pwd/"stats"/ fln, json4("data"))
          //println(i)
        }else
          print("probleme with stats extraction")
        Thread.sleep(1400)
      }
      outp = 0
    } catch {
      case e: Throwable=>println("problems with extraction")
             }
    return outp

  }

}
