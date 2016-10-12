/**
  * Created by projectone on 7/10/16.
  */
import org.apache.spark.{SparkConf, SparkContext}

object WordCount1 {
  def main(rgs:Array[String]) {
    val conf = new SparkConf().setAppName("hhhh").setMaster("local");
    val sc = new SparkContext(conf);

    val lines = sc.parallelize(Seq("Hello this is scala","hello this is spark","hello this is sbt"));
    val counts = lines.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_+_);
    counts.collect.foreach(println);


  }
}
