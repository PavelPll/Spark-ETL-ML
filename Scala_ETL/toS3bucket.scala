// CREATE S3 bucket in the same geographical region as HADOOP and Spark
// SAVE the dataframe df to AWS S3 bucket

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.AmazonClientException
import com.amazonaws.AmazonServiceException
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.model.ObjectMetadata
import java.io.File
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.FileOutputStream
import java.io.ByteArrayInputStream

object toS3bucket {  
  val AWS_ACCESS_KEY = "..."
  val AWS_SECRET_KEY = "..."
  
  // CREATE S3 bucket in the same geographical region as HADOOP and Spark
  def createBucket(s3BucketName: String): Unit={
    val awsCredentials = new BasicAWSCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY)
    val amazonS3Client = new AmazonS3Client(awsCredentials)
    amazonS3Client.listBuckets()
    amazonS3Client.createBucket(s3BucketName, Regions.US_WEST_2.getName)
  }
  
  // SAVE the dataframe df to AWS S3 bucket 
  def toS3(spark:SparkSession, df:DataFrame, fileName:String):Int={
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key",AWS_ACCESS_KEY)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key",AWS_SECRET_KEY)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    df.coalesce(1).write.mode("overwrite").parquet("s3a://bucketpavel4/"+fileName)
    return(1)
  }

}
