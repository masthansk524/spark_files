import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by projectone on 11/10/16.
  */
object SparkSchemaExample1 {
  val conf = new SparkConf().setMaster("local").setAppName("CSV data processing");
  val sc = new SparkContext(conf);

  val sqlc = new org.apache.spark.sql.SQLContext(sc);


  import org.apache.spark.sql.types._
  import org.apache.spark.sql.Row
  val peopleRDD = spark.sparkContext.textFile("examples/src/main/resources/people.txt")
  val peoplerdd = spark.sparkContext.textFile("/home/projectone/spark/input/data1.txt")
  val schemaString = "first_name last_name age";
  // schema structtype
  val fields = schemaString.split(" ")
    .map(fieldname => StructField(fieldname,StringType, nullable = true))

  val schema = StructType(fields);

  //convert records of the rdd to rows

  val rowrdd = peoplerdd.map(w => w.split(",")).map(attributes => Row(attributes(0),attributes(1),attributes(2).toInt))

  val peopledf = spark.createDataFrame(rowrdd,schema);

  peopledf.createOrReplaceTempView("people");
  val result = spark.sql("select * from people");


}
