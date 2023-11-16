// ADD single row to the given SQL table
import scala.sys.process._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.{ForeachWriter, Row}
import java.sql.{Connection, DriverManager, ResultSet};


object toSQL {
  // ADD single row to the given SQL table
  def toSQLp(record: Row): Unit = {
    val dbc = "jdbc:postgresql://172.31.6.205:5432/mydb?user=pavel1&password=p"
    // LOAD the driver
    val conn = DriverManager.getConnection(dbc)
    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
    // INSERT the row
    try {
        val prep = conn.prepareStatement("INSERT INTO etl2 (c0, c1, c2, c3, c4, c5, c6, c7) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ")
        //prep.setTimestamp(1, record.getTimestamp(0))
        prep.setLong(1, record.getLong(0))
        prep.setLong(2, record.getLong(1))
        prep.setLong(3, record.getLong(2))
        prep.setString(4, record.getString(3))
        prep.setLong(5, record.getLong(4))
        prep.setLong(6, record.getLong(5))
        prep.setLong(7, record.getLong(6))
        prep.setLong(8, record.getLong(7))
        prep.executeUpdate
    }
    finally {
        conn.close
    }
    //return(1)
  }


}
