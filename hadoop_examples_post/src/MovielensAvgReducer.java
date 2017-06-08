import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovielensAvgReducer extends Reducer<Text, MapWritable, Text, DoubleWritable> 
{
	@Override
	public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
		
		int cnt = 0;
		double sum = 0;
		
		//TODO: iterate over the values, extract "cnt" and "sum" and accumulate them into cnr and sum.
		//Then emit the result, sum/cnt, using the same key we got in reduce(). 
		//Convert sum/cnt to DoubleWritable (by doing new DoubleWritable(sum/cnt)).
		
	}
}

