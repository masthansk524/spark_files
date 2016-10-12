
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

object SparkHiveExample1 {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("Dividents").setMaster("local");
    val sc = new SparkContext(conf);

      val hc1 = new org.apache.spark.sql.hive.HiveContext(sc);
    //	  val hc = new org.apache.spark.sql

    val hc = new org.apache.spark.sql.SQLContext(sc)


    hc1.sql("create table person(first_name string, last_name string, age int) row format delimited fields terminated by ','")
    hc1.sql("load data local inpath \"/home/projectone/spark/input/data1.txt\" into table person")

    val result = hc1.sql("FROM person SELECT first_name,last_name, age")
    result.collect.foreach(println);
    //result.collect.foreach( t =>
    //{

      //println("name is:" + t(0))

    //}
//    )

    //result.show();
    //result.saveAsTextFile("/home/projectone/spark/output/sqlop1");

  }

}
