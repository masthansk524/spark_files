import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext._
/**
  * Created by projectone on 7/10/16.
  */
case class Person(first_name:String, last_name:String, age:Int)
object SparkSqlExample1 {
  def main(rgs:Array[String]) {
    val conf = new SparkConf().setAppName("hhhh").setMaster("local");
    val sc = new SparkContext(conf);

    val ssc = new org.apache.spark.sql.SQLContext(sc);
    val p = sc.textFile("/home/projectone/spark/input/data1.txt");
    val pmap = p.map(p => p.split(","));
      val perosnrdd = pmap.map(p=> Person(p(0),p(1),p(2).toInt))
    import ssc.implicits._

    val PersonDF = perosnrdd.toDF;
    PersonDF.registerTempTable("persons");
    val Persondf = ssc.sql("select * from persons").collect().foreach(println);


  }
}
