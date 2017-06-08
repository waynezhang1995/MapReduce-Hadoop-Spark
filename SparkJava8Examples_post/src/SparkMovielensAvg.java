import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;

import scala.Tuple2;

public class SparkMovielensAvg {
	
  public static void main(String[] args) throws Exception {
    
    SparkConf conf = new SparkConf()
    .setMaster("local")
    .setAppName("Movielens");
    JavaSparkContext sc = new JavaSparkContext(conf);
    //Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
    sc.setLogLevel("ERROR");
    
    JavaRDD<String> mRDD = sc.textFile("input_movielens"); //directory where the files are
    
    JavaPairRDD<String, Tuple2<Double, Double>> mPairRDD = mRDD.flatMapToPair(
    		line -> {
    			List< //we return a list of K,V pairs (Tuple2) from flatMapToPair 
					Tuple2<
						String, //movieID or userID
						Tuple2<Double,Double> //rating, 1.0
					>
			    >  l = new ArrayList<>();
    			
    			//check to see if this is the header line
    			if(line.contains("userId"))
    				return l.iterator(); //return empty list
    			
    			String[] lineArr = line.split(",");
    			String userID = lineArr[0];
    			String movieID = lineArr[1];
    			Double rating = Double.parseDouble(lineArr[2]);
    			
    			l.add(new Tuple2<>("m"+movieID, new Tuple2<>(rating,1.0)));
    			//TODO add a similar tuple for userID
    			
    			return l.iterator();
    		})
    		.aggregateByKey(
    		    	new Tuple2<Double, Double>(0.0, 0.0), 
    				(acc, value) -> new Tuple2<>(acc._1 + value._1, acc._2 + 1),
    				(acc1, acc2) -> new Tuple2<>(acc1._1 + acc2._1, acc1._2 + acc2._2) );
    
    
    JavaPairRDD<String,Double> avgPairRDD = mPairRDD.mapToPair(x->new Tuple2<>(x._1, x._2._1/x._2._2));
    
	//remove output directory if already there
	FileSystem fs = FileSystem.get(sc.hadoopConfiguration());
	fs.delete(new Path("output_movielens"), true); // delete dir, true for recursive
	avgPairRDD.saveAsTextFile("output_movielens");
	
	System.out.println("Done. See result in 'output_movielens'");
    	
    sc.stop();
    sc.close();
  }
}