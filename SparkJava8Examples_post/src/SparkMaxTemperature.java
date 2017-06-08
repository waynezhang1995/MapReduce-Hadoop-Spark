import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;

import scala.Tuple2;

public class SparkMaxTemperature {
	
  private static final int MISSING = 9999;
	
  public static void main(String[] args) throws Exception {
    
    SparkConf conf = new SparkConf()
    .setMaster("local")
    .setAppName("Max Temperature");
    JavaSparkContext sc = new JavaSparkContext(conf);
    //Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
    sc.setLogLevel("WARN");
    
    JavaRDD<String> weatherRDD = sc.textFile("input_weather"); //directory where the files are
    
    JavaPairRDD<String, Integer> yeartemp = weatherRDD.mapToPair(
    		value -> {
    			String line = value.toString();
    			String year = line.substring(15, 19);
    			int airTemperature;
    			
    			if (line.charAt(87) == '+') // parseInt doesn't like leading plus signs
    				airTemperature = Integer.parseInt(line.substring(88, 92));
    			else
    				airTemperature = Integer.parseInt(line.substring(87, 92));
    			
    			String quality = line.substring(92, 93);
    			if (airTemperature != MISSING && quality.matches("[01459]"))
    				return new Tuple2<>(year,airTemperature);
    			return null;
    		})
    		.filter(x->x!=null)
    		.reduceByKey((x,y)->{if(x>y) return x; else return y;});
    
    System.out.println(yeartemp.collect());
    
	//remove output directory if already there
	FileSystem fs = FileSystem.get(sc.hadoopConfiguration());
	fs.delete(new Path("output_weather"), true); // delete dir, true for recursive
    //yearmax.saveAsTextFile("output_weather");
    	
    sc.stop();
    sc.close();
  }
}