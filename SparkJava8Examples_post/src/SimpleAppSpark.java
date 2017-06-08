import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;

public class SimpleAppSpark {
	
  public static void main(String[] args) {
    
    SparkConf conf = new SparkConf()
    .setMaster("local")
    .setAppName("Simple Application");
    JavaSparkContext sc = new JavaSparkContext(conf);
    //Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
    sc.setLogLevel("WARN");
    
    JavaRDD<String> input = sc.textFile("war_and_peace.txt");

    long numAs = input.filter(s -> s.contains("a")).count();

    long numBs = input.filter(s -> s.contains("b")).count();

    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
    
    sc.stop();
    sc.close();
  }
}