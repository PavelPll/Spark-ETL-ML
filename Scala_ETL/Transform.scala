
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import Extract._
import toHDFS._

object Transform {

  val spark = SparkSession.builder()
      .appName("movies")
//      .master("local[*]")
      .master("yarn")
      .getOrCreate()

  def main(args: Array[String]) {
    /*
    // EXTRACTION
    // Extraction of the web page 1 together with all its pages to ./folder on masternode
    val web1:String="https://www.balldontlie.io/api/v1/games?seasons[]=2021"
    var folder = "games"
    println("Exit code of extraction is:"+json_extraction(web1, folder) )
    //Extraction of the web page 2 together with all its pages to ./folder on masternode
    val web2 = "https://www.balldontlie.io/api/v1/stats?seasons[]=2021"
    folder = "stats"
    println("Exit code of extraction is:"+json_extraction(web2, folder) )
    */

    //TRANSFER from MASTER to HDFS
    //println(to_HDFSF("games", "hdfs:///scala"))
    //println(to_HDFSF("stats", "hdfs:///scala"))
    //println(os.Path) //Path(os.pwd+"/"+"games") ))
    //println(os.pwd)
    //println(os.list(os.pwd/"games"))

    val conf = new Configuration()
    val hdfs = FileSystem.get(conf)
    val p = new Path("/scala/games")
    val ls = hdfs.listStatus(p)
    val filelist = ls.map(x => x.getPath.toString)
    filelist.foreach(println)
    println(filelist.getClass)
    println(filelist{0})

    //var games0 = spark.read.option("inferSchema","true").option("multiline","true").json("/scala/games/games_1.json")
    //filelist.foreach( games0 = games0.union(spark.read.option("inferSchema","true").option("multiline","true").json(_) )  )
    
    //println("ls: ", ls.foreach( x => println(x.getPath) ))
    //println("ls0: ", ls{0}.getpath)
    //println("ls0: ", ls{0})
    //println("ls0: ", ls{0}.getPath.toString)

    //val path = new Path("hdfs:///scala/games/")
    //val files = hdfs.listFiles(path, false)
    //print("list: ", files{0}) //, files.getClass)
    //println("ls: ", ls{0}.getClass)
    //println(Files.list(os.pwd).iterator().asScala.foreach(println).getClass)

    //union json to dataframe
    //filelist.remove(1)
    
    //var games0 = spark.read.option("inferSchema","true").option("multiline","true").json("/scala/games/games_1.json")
    var games0 = spark.read.option("inferSchema","true").option("multiline","true").json(filelist{0})
    for( i <- 1 to (filelist.length-1) ){
    //for( fle <- filelist){
      var games01 = spark.read.option("inferSchema","true").option("multiline","true").json(filelist{i})
      games0 = games0.union(games01)
    }
/*
    var stats0 = spark.read.option("inferSchema","true").option("multiline","true").json("/scala/stats/stats_1.json")
    for( i <- 2 to 200){
      var stats01 = spark.read.option("inferSchema","true").option("multiline","true").json(s"/scala/stats/stats_${i}.json")
      stats0 = stats0.union(stats01)
      println(i)
    }
  //game preprocessing
  games0.printSchema
  
  val games1 = games0.select(col("home_team_score"), col("id").as("id_match"), col("home_team.id").as("id_equipe"), col("home_team.name").as("nom_equipe") )
  
  val equipes = List("Suns", "Hawks", "Lakers", "Bucks")
  val games2 = games1.filter(col("nom_equipe").isin(equipes: _*))
  //stats preprocessing
  val stats2 = stats0.select(col("game.id").as("id_matchS"), col("team.id").as("id_equipeS"), col("pts"), col("reb"), col("ast"), col("blk"))
  //left join to keep on lines because stats is not comlete
  val df = games2.join(stats2, games2("id_match")===stats2("id_matchS") && games2("id_equipe")===stats2("id_equipeS"), "left_outer").drop("id_matchS").drop("id_equipeS")
  //write to hdfs, not to master
  //file in many parts without coalesce(1)
  df.coalesce(1).write.csv("/data/scala/output200.csv")  
  df.count()
  df.show(3)
  */
  }

}
