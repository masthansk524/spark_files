//import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.SparkContext._

/**
  * Created by projectone on 8/10/16.
  */

object SparkSqlExample2 {


  case class Person(fname:String, lname:String, age:Int)
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local");

    val sc = new SparkContext(conf);

    val sqlc = new org.apache.spark.sql.SQLContext(sc);

    val data = sc.textFile("/home/projectone/spark/input/data1.txt");

    val data2 = data.map(p => p.split(","));
    val data3 = data2.map(p=> Person(p(0),p(1),p(2).toInt));

    import sqlc.implicits;
    import sqlc.implicits._
    val personDF = data3.toDF // RDD+Schema
    personDF.printSchema
    personDF.registerTempTable("person");

    val result = sqlc.sql("select * from person");

    result.collect().foreach(println);
  }
}
