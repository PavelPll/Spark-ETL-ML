import scala.sys.process._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.Dataset


object df_union {
  // reads all json files in path_hdfs as DataFrames
  // unions all DataFrames to one
  def df_unionF(spark:org.apache.spark.sql.SparkSession, path_hdfs:String):org.apache.spark.sql.DataFrame={

    val conf = new Configuration()
    val hdfs = FileSystem.get(conf)
    val path1 = new Path(path_hdfs)
    val ls = hdfs.listStatus(path1)
    val filelist = ls.map(x => x.getPath.toString)
    filelist.foreach(println)

    println("ls: ", ls.foreach( x => println(x.getPath) ))
    println("ls0: ", ls{0}.getPath)

    var games0 = spark.read.option("inferSchema","true").option("multiline","true").json(filelist{0})
    var seq1 = Seq(games0)
    for( i <- 1 to (filelist.length-1) ){
    //for( fle <- filelist){
      var games01 = spark.read.option("inferSchema","true").option("multiline","true").json(filelist{i})
      // var games01 = spark.read.option("inferSchema","true").option("multiline","true").json(fle)
      seq1 = seq1 :+ games01
      //games0 = games0.union(games01)
    }
    val df1 = seq1.reduce(_ union _)
    println("df1: ", df1.count())
    //println("type: ", df1.getClass)
    //df1.show()

  return(df1)

}

}


