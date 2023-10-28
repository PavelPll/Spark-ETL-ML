
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import Extract._
import toHDFS._
import df_union._

object ETL {

  val spark = SparkSession.builder()
      .appName("movies")
//      .master("local[*]")
      .master("yarn")
      .getOrCreate()

  def main(args: Array[String]) {
    
    // EXTRACTION
    // Extraction of the web page 1 together with all its pages to ./folder on masternode
    val web1:String="https://www.balldontlie.io/api/v1/games?seasons[]=2021"
    var folder = "games"
    println("Exit code of extraction is:"+json_extraction(web1, folder) )
    //Extraction of the web page 2 together with all its pages to ./folder on masternode
    val web2 = "https://www.balldontlie.io/api/v1/stats?seasons[]=2021"
    folder = "stats"
    println("Exit code of extraction is:"+json_extraction(web2, folder) )


    // TRANSFER from MASTER to HDFS
    println(to_HDFSF("games", "hdfs:///scala"))
    println(to_HDFSF("stats", "hdfs:///scala"))
    //println(os.list(os.pwd/"games"))

    // CONVERT all json files to DataFrames and union them
    val games0 = df_unionF(spark, "/scala/games")
    val stats0 = df_unionF(spark, "/scala/stats")

    // PREPROCESSING of the DataFrame from web1
    // games0.printSchema  
    val games1 = games0.select(col("home_team_score"), col("id").as("id_match"), col("home_team.id").as("id_equipe"), col("home_team.name").as("nom_equipe") )
    val equipes = List("Suns", "Hawks", "Lakers", "Bucks")
    val games2 = games1.filter(col("nom_equipe").isin(equipes: _*))
    // PRPROCESSING of the DataFrame from web2
    val stats2 = stats0.select(col("game.id").as("id_matchS"), col("team.id").as("id_equipeS"), col("pts"), col("reb"), col("ast"), col("blk"))

    // JOIN two DataFrames
    // left join to keep on lines because stats is not comlete
    val df = games2.join(stats2, games2("id_match")===stats2("id_matchS") && games2("id_equipe")===stats2("id_equipeS"), "left_outer").drop("id_matchS").drop("id_equipeS")

    //LOAD
    //write back to hdfs as a single file for further use by datascientist for example
    //file in many parts without coalesce(1)
    df.coalesce(1).write.csv("/data/scala/output_10d.csv")  
    //df.count()
    df.show(10)
  
  }

}
