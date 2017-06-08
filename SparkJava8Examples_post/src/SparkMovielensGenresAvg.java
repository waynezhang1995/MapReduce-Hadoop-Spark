import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkFiles;

import scala.Tuple2;

public class SparkMovielensGenresAvg {
	
  public static void main(String[] args) throws Exception {
    
    SparkConf conf = new SparkConf()
    .setMaster("local")
    .setAppName("Movielens");
    JavaSparkContext sc = new JavaSparkContext(conf);
    //Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
    sc.setLogLevel("ERROR");
    
    //copies the file to all the machines
    sc.addFile("movielens/ml-latest-small/movies.csv");
    
    Map<String, String[]> movieGenres = new HashMap<>();
    
	String localPath = SparkFiles.get("movies.csv");
	System.out.println(localPath);
	
    BufferedReader in = new BufferedReader(new FileReader(new File(localPath)));
    
    //TODO: fill in movieGenres
    
    in.close();
    
    JavaRDD<String> mRDD = sc.textFile("input_movielens"); //directory where the files are
    
    JavaPairRDD<String, Tuple2<Double, Double>> mPairRDD = null; //TODO = ...    
    
    JavaPairRDD<String,Double> avgPairRDD = null; //TODO = ...
    
	//remove output directory if already there
	FileSystem fs = FileSystem.get(sc.hadoopConfiguration());
	fs.delete(new Path("output_movielens"), true); // delete dir, true for recursive
	avgPairRDD.saveAsTextFile("output_movielens");
	
	System.out.println("Done. See result in 'output_movielens'");
    	
    sc.stop();
    sc.close();
  }
}