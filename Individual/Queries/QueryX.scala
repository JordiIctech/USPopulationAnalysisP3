import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import java.io.File
import scala.io.Source

object QueryX {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("Created spark session.")

    val testing1 = spark.read.format("csv").option("header","true").load("D:\\Revature\\DowloadDataScala\\FinalExport\\ObtainedETLDataV1\\Combine2000RG.csv") // File location in hdfs
    testing1.createOrReplaceTempView("Testing1Imp")

    val testing2 = spark.read.format("csv").option("header","true").load("D:\\Revature\\DowloadDataScala\\FinalExport\\ObtainedETLDataV1\\Combine2010RG.csv") // File location in hdfs
    testing2.createOrReplaceTempView("Testing2Imp")

    val testing3 = spark.read.format("csv").option("header","true").load("D:\\Revature\\DowloadDataScala\\FinalExport\\ObtainedETLDataV1\\Combine2020RG.csv") // File location in hdfs
    testing3.createOrReplaceTempView("Testing3Imp")

    val headers = spark.read.format("csv").option("header","true").load("D:\\Revature\\DowloadDataScala\\FinalExport\\ObtainedETLDataV1\\Headers.csv") // File location in hdfs
    headers.createOrReplaceTempView("HeaderImp")

    val t1 = System.nanoTime

    var testingS = testing1.drop("FILEID", "STUSAB", "Region", "Division", "CHARITER", "CIFSN", "LOGRECNO")

    var ColumnNames=testingS.columns
    var Columnstring = ColumnNames.mkString("sum(", "),sum(", ")")
    var Columnstring2 = ColumnNames.mkString(",")
    var Columnlist = Columnstring.split(",")

    var HeaderNames = headers.columns
    var Headerstring = HeaderNames.mkString(",")
    var Headerlist = Headerstring.split(",")

    val testingE1= spark.sql(s"SELECT $Columnstring FROM Testing1Imp")

    val testingE2 = spark.sql(s"SELECT $Columnstring FROM Testing2Imp")

    val testingE3 = spark.sql(s"SELECT $Columnstring FROM Testing3Imp")

    var Join1 = testingE1.union(testingE2)

    var Join2 = Join1.union(testingE3)


    var lastimp = Columnlist.length

    for ( i <- 0 until lastimp){

      var FinalTable = Join2.withColumnRenamed(s"${Columnlist(i)}",f"${Headerlist(i).dropRight(1)}")

      Join2 = FinalTable

    }

    Join2.coalesce(1).write.option("header", "true").csv("D:\\Revature\\DowloadDataScala\\FinalExport\\OutputCSV2\\QueryX")

    //file.outputcsv("QueryX", Join2)

    val duration = (System.nanoTime - t1)
    println("Code Lasted: " + (duration/1000000000) + " Seconds")
  }
}

