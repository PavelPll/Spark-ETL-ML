
import scala.sys.process._

object toHDFS {
  //put ./games/* ans ./stats/* from master to HDFS
  //does not work because I have no executors on master
  def to_HDFSF():Int={

  val cmd1 = "hdfs dfs -put /home/ubuntu/spark/my/my3/games/* /scala/games/"
  //val cmd1 = "pwd"
  val output1 = cmd1.!!
  val cmd2 = "hdfs dfs -put /home/ubuntu/spark/my/my3/stats/* /scala/stats/"
  val output2 = cmd2.!!
  return(1)

}

}