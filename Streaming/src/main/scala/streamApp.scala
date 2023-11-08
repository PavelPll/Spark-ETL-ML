import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType, ArrayType, TimestampType}
import org.apache.spark.sql.streaming.{Trigger, OutputMode}

//import org.apache.spark.sql.{ForeachWriter, Row}

//import java.sql.{Connection, DriverManager, ResultSet};
import toSQLstreaming.PostgreSqlSink

object Streaming {

    def main(args: Array[String]) {

        val conf = new SparkConf()
        .setAppName("SparkStreaming")
        .set("spark.streaming.stopGracefullyOnShutdown", "true")

        val spark = SparkSession.builder()
        .master("yarn")
        .config(conf)
        .getOrCreate()
        
    //    spark.sparkContext.setLogLevel("ERROR") // Pour ne pas avoir des informations inutiles dans le terminal lors de l'exécution.
    import spark.implicits._

    // READ the initial batch as a dataframe
    val schema = new StructType()
        .add("time", TimestampType)
        .add("make", StringType, true)
        .add("num-of-cylinders", StringType, true)
        .add("composed", new StructType().add("fuel-system", StringType).add("price", IntegerType) ) 
    val df = spark.readStream
        .schema(schema)
        .option("maxFilesPerTrigger", 2)
        .json("/scala/data_flow/")

    // (1) CONVERT "arrays" to columns
    val df_clean = df.select(
        $"time" as "time",
        $"make" as "make",
        $"num-of-cylinders" as "num-of-cylinders",
        col("composed").getItem("fuel-system") as "fuel-system",
        col("composed").getItem("price") as "price")
    df_clean.printSchema
    //df_clean.withColumn("price",col("price").cast("integer"))
    //df_clean.select(col("price").cast("int").as("price"))

    // (2) FILTER some rows
    val df_filtre = df_clean.filter(($"price" > 5000) && ($"price"<100000))
//    val price_filtre = df_clean.filter(($"price" > 8) && ($"price" < 10) && ($"shop" === "Pizza Hut"))

    // (3) AGGREGATION sum mean count min
    val df_aggregate = df_filtre.groupBy("price").agg(mean("price"))

    // (4) JOIN dynamic dataframe with the static one
    val schema_batch = new StructType()
        .add("num-of-cylinders", StringType)
        .add("value", IntegerType)
    var df_static = spark.read
        .option("schema_batch","true")
        .json("/scala/data_static/data_static.json")
    df_static = df_static.withColumnRenamed("num-of-cylinders","num-of-cyl")
    df_static.show
    //val df_join = df_filtre.join(df_static, Seq("num_str", "num-of-cylinders"), "left_outer").drop("num_str")
    val df_join = df_filtre.join(df_static, df_static("num-of-cyl")===df_filtre("num-of-cylinders"), "left_outer").drop("num-of-cyl")

// STREAMING different kinds of created above dataframes
// EITHER the output to console or to POSTGRESQL
// Uncomment the right one

//    df_filtre.writeStream //0raw data, 1array, 2filtre
//        .trigger(Trigger.ProcessingTime("10 seconds"))
//        .outputMode("append")
//        .format("console")
//        .start()
//        .awaitTermination()

//    df_aggregate.writeStream //3aggr
//        .outputMode("complete")
//        .format("console")
//        .start()
//        .awaitTermination

//    df_join.writeStream //4join
//        .outputMode("update")
//        .format("console")
//        .trigger(Trigger.ProcessingTime("10 seconds"))
//        .start()
//        .awaitTermination

    val df_with_window = df_filtre
          .groupBy(window($"time", "30 seconds", "15 seconds"), $"make", $"num-of-cylinders")
          .agg(sum($"price").as("sum_price"))
    val df_join_window = df_with_window.join(df_static, df_static("num-of-cyl")===df_with_window("num-of-cylinders"), "left_outer")
          .select($"window.start" as "start", $"window.end" as "end", $"make", $"sum_price", $"num-of-cylinders", $"value")

    // WRITE streaming data to pstgreSQL
    val jdbcWriter = new PostgreSqlSink()
    val query = df_join_window.writeStream
        .foreach(jdbcWriter)
//        .format("console")
        .trigger(Trigger.ProcessingTime("10 seconds"))
        .outputMode("update")
        .start()
        .awaitTermination()


    }
}
  
