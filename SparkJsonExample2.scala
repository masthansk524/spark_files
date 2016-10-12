/**
  * Created by projectone on 11/10/16.
  */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object SparkJsonExample2 {

  case class Person(first_name:String, last_name:String, age:Int);
  def main(args:Array[String]) {

    val conf = new SparkConf().setMaster("local").setAppName("CSV data processing");
    val sc = new SparkContext(conf);

    val sqlc = new org.apache.spark.sql.SQLContext(sc);


    import sqlc.implicits._

    val person1 = sqlc.jsonFile("/home/projectone/json1.json");

    //val person3 = sqlc.jsonFile("hdfs://localhost:9000/spark/sparkinputs/json1.json");

    val person2 = sqlc.read.json("/home/projectone/json1.json");

//    person3.printSchema();
    // applying schema to the DF

    //val data = person.map()

    person1.registerTempTable("json_person");

    val result = sqlc.sql("select * from json_person");

    //result.collect.foreach(println);
    val result2 = sqlc.sql("select * from json_person where age > 60");

    result2.collect.foreach(println);

    result2.toJSON.saveAsTextFile("/home/projectone/spark/input/json2op3");


  }
}
