import scala.sys.process._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
// import org.apache.hadoop.fs
// import org.apache.hadoop.fs._

object toHDFS {
  // copy some folders with its content to /scala
  // from masternode to hdfs 
  def to_HDFSF(folder:String, path_hdfs:String):Int={
    val conf = new Configuration()
    val hdfs = FileSystem.get(conf)
    // copy games  
    //var localpath = new Path("file:///home/ubuntu/spark/my/my3/games")
   var localpath = new Path(os.pwd+"/"+folder)
   var hdfspath = new Path(path_hdfs)
  hdfs.copyFromLocalFile(localpath, hdfspath)

  // The second approach to write in HDFS
  val filename = new Path("file.json")
  val os1 = hdfs.create(new Path("hdfs://masternode:9000/scala/file.txt"))
  os1.write("some strings".getBytes)
  hdfs.close()

  // The third approach, trying cmd, below does not work
  //val cmd1 = "hdfs dfs -put /home/ubuntu/spark/my/my3/games/* /scala/games/"
  //val cmd1 = Seq("hdfs","dfs","-put","/home/ubuntu/spark/my/my3/games/*","/")
  //val output1 = cmd1.!!

  return(1)

}

}


