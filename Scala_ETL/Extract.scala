import scala.sys.process._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object Extract {

  // Extraction of the web page together with all its pages to ./folder on masternode
  def json_extraction(web:String, folder:String):Int={
    val mkdir = os.proc("mkdir", os.pwd+"/"+folder).call()
    println(mkdir.exitCode)
    var outp: Int = 1
    try {
      var r1 = requests.get(web)
      var json1 = ujson.read(r1.text)
      val Npages = json1("meta")("total_pages").num.toInt
      for( i <- 1 to Npages){
        var URL = web+s"&page=${i}" 
        println(URL)
        r1 = requests.get(URL)
        if (r1.statusCode == 200){
          json1 = ujson.read(r1.text)
          var fln = folder + s"_${i}.json"
          os.write(os.pwd/folder/ fln, json1("data"))
          // println("Type of var1: "+json1.getClass)
          println("Page: "+i+" is saved on master")
        }else
          print("problem with web extraction")
        Thread.sleep(1400)
      }
      outp = 0
    } catch {
      case e: Throwable=>println("problems with extraction")
             }
    return outp

  }

}


