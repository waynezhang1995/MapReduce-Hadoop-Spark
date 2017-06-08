import java.util.Arrays;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;

import scala.Tuple2;

public class SparkWordCount {
	
  public static void main(String[] args) throws Exception {
    
    SparkConf conf = new SparkConf()
    .setMaster("local")
    .setAppName("Simple Application");
    JavaSparkContext sc = new JavaSparkContext(conf);
    //Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
    sc.setLogLevel("WARN");
    
    JavaRDD<String> textFile = sc.textFile("war_and_peace.txt");
    JavaRDD<String> stopwords = sc.textFile("stopwords.txt");
    
    JavaPairRDD<String, Integer> counts = textFile
        .flatMap(s -> Arrays.asList(s.split("[\\P{L}]+")).iterator())
        .mapToPair(word -> new Tuple2<>(word.toLowerCase(), 1))
        .subtractByKey(stopwords.mapToPair(x->new Tuple2<>(x, 1)))
        .reduceByKey((a, b) -> a + b);
    
    
	//remove output directory if already there
	FileSystem fs = FileSystem.get(sc.hadoopConfiguration());
	fs.delete(new Path("war_and_peace_word_counts"), true); // delete dir, true for recursive
    counts.saveAsTextFile("war_and_peace_word_counts");
    	
    System.out.println("Done. See result in 'war_and_peace_word_counts'");
    
    sc.stop();
    sc.close();
  }
}