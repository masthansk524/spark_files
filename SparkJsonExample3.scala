/**
  * Created by projectone on 11/10/16.
  */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.SQLContext
object SparkJsonExample3 {
  def main(args:Array[String]) {

      val conf = new SparkConf().setAppName("dif Json format").setMaster("local");
      val sc = new SparkContext(conf);
      val sqlc = new org.apache.spark.sql.SQLContext(sc);

      val cust_schema = (new StructType).fields("payload", (new StructType).fields("event", (new StructType).add("action",String).fields("timestamp",Long)))
      val data = sqlc.read.schema(cust_schema).json("/home/projectone/spark/json2.json");

      data.printSchema();

      data.registerTempTable("json_person2");

      val result = sqlc.sql("select * from json_person2");
    result.collect.foreach(println);



  }
}

