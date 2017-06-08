import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;

import scala.Tuple2;

public class SparkBook {
	
  public static void main(String[] args) throws Exception {
    
    SparkConf conf = new SparkConf()
    .setMaster("local")
    .setAppName("Simple Application");
    JavaSparkContext sc = new JavaSparkContext(conf);
    //Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
    sc.setLogLevel("WARN");
    
    JavaRDD<String> textFile = sc.textFile("war_and_peace.txt");

    JavaRDD<String> N = textFile.filter(x -> x.contains("Natasha"));
    JavaRDD<String> P = textFile.filter(x -> x.contains("Pierre"));
    JavaRDD<String> A = textFile.filter(x -> x.contains("Andrew"));
    JavaRDD<String> NaP = N.intersection(P);
    JavaRDD<String> NaA = N.intersection(A);
    JavaRDD<String> NaPoNaA = NaP.union(NaA);
    
    //NaPoNaA.collect().forEach(x -> System.out.println(x));
    
    
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
    JavaRDD<Integer> result = rdd.map(x -> x*x);
    result.collect().forEach(y -> System.out.println(y));
    
    
    JavaRDD<String> words = textFile.flatMap(x->Arrays.asList(x.split("[\\P{L}]+")).iterator());
    
    //words.foreach(x -> System.out.println(x));
    
    textFile.map(x->Arrays.asList(x.split("[\\P{L}]+")).iterator()).foreach(
	    		iterator -> {
	    			List<String> list = new ArrayList<>();
	    			iterator.forEachRemaining(list::add);
	    			//System.out.println(list);
	    		}
    		);
    
    
    Integer sum = rdd.reduce((x,y)->x+y);
    System.out.println(sum);
    
    //computing average
    Tuple2<Integer, Integer> sumcnt = 
    		rdd.map(x->new Tuple2<>(x,1))
    		   .reduce((t,r)->new Tuple2<>(t._1+r._1, t._2+r._2));
    double avg = 1.0*sumcnt._1 / sumcnt._2;
    System.out.println(avg);
    
    //computing average with aggregate()
    sumcnt = rdd.aggregate(
    				new Tuple2<>(0, 0), 
    				(acc, value) -> new Tuple2<>(acc._1 + value, acc._2 + 1),
    				(acc1, acc2) -> new Tuple2<>(acc1._1 + acc2._1, acc1._2 + acc2._2) 
    				);
    avg = 1.0*sumcnt._1 / sumcnt._2;
    System.out.println(avg);
    
    JavaDoubleRDD wordLength = words.mapToDouble(x->x.length());
    double wordAvgLength = wordLength.mean();
    System.out.println(wordAvgLength);
    
    JavaPairRDD<Integer, String> lw = words.mapToPair( x -> new Tuple2<>(x.length(),x) );
    Map<Integer, Long> r = lw.countByKey();
    System.out.println(r);
    
    JavaPairRDD<Integer, Iterable<String>> longwordsRDD = lw.groupByKey().filter(x -> x._1 >= 12);
    List<Tuple2<Integer, Iterable<String>>> longwordsList = longwordsRDD.collect();
    System.out.println(longwordsList);
    
    
    sc.stop();
    sc.close();
  }
}