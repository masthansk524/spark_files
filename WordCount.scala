import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local");

    val sc = new SparkContext(conf);

    if(args.length < 2 ) {
      println("Invaliad argument");
    }
    // load text file into RDD
    val textFile = sc.textFile("/home/projectone/spark/input/wc1.txt");
//    val textFile = sc.textFile(args(0));
    //read line and split into words based on space delimiter
    val lines = textFile.flatMap(line => line.split(" "));
    //assign count1 to each word
    val words = lines.map(word => (word,1));
    // group by key and calculate the sum with repeat times
    val count = words.reduceByKey(_+_);
    // sort the data by key
    val word_sort = count.sortByKey();
    // print the result
    word_sort.collect.foreach(println);
    // print the result into local file system
    word_sort.saveAsTextFile("/home/projectone/spark/output/wc111");
//    word_sort.saveAsTextFile(args(1));


  }






}