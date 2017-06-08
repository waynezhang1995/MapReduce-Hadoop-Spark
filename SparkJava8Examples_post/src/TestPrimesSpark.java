import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

//539779
//Time elapsed = 20 sec

//With chopping up to 20 partitions
//539779
//Time elapsed = 9.7
//I believe the overhead compared to J8 is because of Scala implementation of Spark.

public class TestPrimesSpark {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
						    .setMaster("local[*]")
						    .setAppName("Simple Application");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("WARN");
		
		long startTime = System.currentTimeMillis();
		long estimatedTime;
		
		int max = 8000000;
		
		List<Integer> listInt = new ArrayList<Integer>();
		for(int i=0; i<max; i++)
			listInt.add(i);
		
		System.out.println(
				sc.parallelize(listInt, 20)
						.filter(n -> 
						{
							for(int i=2; i<=(int)Math.sqrt(n); i++)
								if(n % i == 0)
									return false;
							return true;
						}).count()
				);
		
		estimatedTime = System.currentTimeMillis() - startTime;
		System.out.println("Time elapsed = " + estimatedTime / 1000.0);
		
		sc.stop();
		sc.close();
	}
}
