import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import com.databricks.spark.csv

/**
  * Created by projectone on 8/10/16.
  */
object SparkCSVJsonexample1 {

  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local");

    val sc = new SparkContext(conf);

    val sqlc = new org.apache.spark.sql.SQLContext(sc);

//    val ntdf = sqlc.load("com.databricks.spark.csv", Map("path"->args(0),"header"->true))

    val persondf = sqlc.jsonFile("/home/projectone/spark/input/one.csv");

    persondf.printSchema();
    persondf.registerTempTable("stock");
    val query = sqlc.sql("select * from stock");
    query.collect.foreach(println);
    val query1 = sqlc.sql("select * from stock where ")

  }
}
