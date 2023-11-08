// I used the following ref to write down this class
// https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/ForeachWriter.html
// https://3rdman.de/2019/08/spark-to-postgresql-real-time-data-processing-pipeline-part-5/
// https://spark.apache.org/docs/2.1.0/api/java/org/apache/spark/sql/Row.html
// https://docs.oracle.com/javase/8/docs/api/java/sql/PreparedStatement.html
// https://www.postgresql.org/docs/current/datatype.html

import scala.sys.process._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.Dataset
import java.sql.{Connection, DriverManager, ResultSet};

object toSQLstreaming {

  class PostgreSqlSink() extends org.apache.spark.sql.ForeachWriter[org.apache.spark.sql.Row] {
    var connection: java.sql.Connection = _
    var statement: java.sql.PreparedStatement = _

    def open(partitionId: Long, version: Long): Boolean = {
      val dbc = "jdbc:postgresql://172.31.6.205:5432/mydb?user=pavel1&password=p"
      connection = DriverManager.getConnection(dbc)
      connection.setAutoCommit(false)
      statement = connection.prepareStatement("INSERT INTO streaming5 (start, ends, make, sum_price, cylinders, value) VALUES (?, ?, ?, ?, ?, ?) ")
      true
    }

    def process(record: org.apache.spark.sql.Row): Unit = {
      statement.setTimestamp(1, record.getTimestamp(0))
      statement.setTimestamp(2, record.getTimestamp(1))
      statement.setString(3, record.getString(2))
      statement.setLong(4, record.getLong(3))
      statement.setString(5, record.getString(4))
      statement.setLong(6, record.getLong(5))
      statement.executeUpdate()        
    }

    def close(errorOrNull: Throwable): Unit = {
      connection.commit()
      connection.close
    }
  }

}
