import java.text.SimpleDateFormat

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import org.apache.poi.ss.usermodel.{Cell, DateUtil}
import org.apache.poi.ss.util.NumberToTextConverter
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.types.{DataTypes, StringType, StructField, StructType}

object main {


  def main(args: Array[String]): Unit = {


    println("hello from Scala!")
    println("version_2!")


    val conf = new SparkConf().setAppName("aa").setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName("demo").master("local").getOrCreate()

    println("hi from spark ")
    println(spark)
    println("version:2")

    //        storing the source i.e s3 file credentials
    //    / storing the source i.e s3 file credentials


    val access_key = ""
    val password = ""
//    val access_key = System.getenv("AWS_ACCESS_KEY")
//    val password = System.getenv("AWS_SECRET_KEY")
    val yourAWSCredentials = new BasicAWSCredentials(access_key, password)
    val amazonS3Client = new AmazonS3Client(yourAWSCredentials)
    // save s3 file in the variable
    val data = amazonS3Client.getObject("bucketofrishi", "dataset.xlsx")
    //    adding functionality of POI worbook to save the excel in collection List[List[String]]
    val myWorkbook = new XSSFWorkbook(data.getObjectContent())
    //    creating collection
    var list_collection = List[List[String]]();
    val mySheet = myWorkbook.getSheetAt(0)
    val rowIterator = mySheet.iterator()
    //date format
    var dff = new SimpleDateFormat("dd/MM/yyyy")
    //Interation over each row and column to store excel data into collection
    while (rowIterator.hasNext) {
      val row = rowIterator.next()
      val cellIterator = row.cellIterator()
      var object_collection = List[String]()
      while (cellIterator.hasNext) {
        val cell = cellIterator.next()
        cell.getCellType match {
          case Cell.CELL_TYPE_STRING => {
            object_collection = object_collection :+ cell.getStringCellValue
          }
          case Cell.CELL_TYPE_NUMERIC => {
            if (DateUtil.isCellDateFormatted(cell))
              object_collection = object_collection :+ dff.format(cell.getDateCellValue).toString()
            else
              object_collection = object_collection :+ (NumberToTextConverter.toText(cell.getNumericCellValue))
          }
          case Cell.CELL_TYPE_BOOLEAN => {
            cell.getBooleanCellValue + "\t"
          }
          case Cell.CELL_TYPE_BLANK => {
            "null" + "\t"
          }
          case _ => {
            throw new RuntimeException(" this error occured when reading ")
          }
        }
      }
      //      insert all object data ie collection from each row into
      list_collection = list_collection :+ object_collection
      //      println()
    }
    //    removing the headers from list
    var final_output = list_collection.toList.tail
    println(final_output)

    //spark_stuff
    println("spark_stuff")
    import spark.implicits._
    //    Intial dataframe create straight from excel read collection to view its data-structure.
    //    val initial_df = final_output.toDF("aa")
    //    initial_df.show()

    //    creating a schema to parse the data of list[list] type
    val final_schema = StructType(
      List(
        StructField("name", StringType, true),
        StructField("age", StringType, true),
        StructField("height", StringType, true),
        StructField("POB", DataTypes.StringType, false),
        StructField("DOB", DataTypes.StringType, false)
      )
    )

    // creating rdd and then dataframe out of that-
    val rdd = sc.parallelize(final_output)
    val rowRdd = final_output.map(v => Row(v: _*))
    val output_df = spark.createDataFrame(sc.parallelize(rowRdd), final_schema);
    val df = output_df.withColumn("age", $"age".cast(sql.types.IntegerType))
      .withColumn("height", $"height".cast(sql.types.FloatType))
      .withColumn("DOB", to_date($"DOB", "MM/dd/yyyy"))


    println(df.printSchema())
    println("dataframe ready to ingest over snowflake warehouse " + df.show())

    //    snowflake ingestion
    var sfOptions = Map(
      "sfURL" -> "ag51129.ap-southeast-1.snowflakecomputing.com",
      "sfUser" -> "cmeshanalytics",
      "sfPassword" -> "Sadhgun@496",
      "sfDatabase" -> "practice_sz",
      "sfSchema" -> "PUBLIC",
      "sfWarehouse" -> "compute_wh"
    )

    df.write
      .format("net.snowflake.spark.snowflake")
      .options(sfOptions)
      .option("dbtable", "table_final")
      .save()

    sc.stop();
  }

}
