import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovielensAvgCombiner extends Reducer<Text, MapWritable, Text, MapWritable> 
{
	@Override
	public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
		
		int cnt = 0;
		double sum = 0;
		
		for (MapWritable value : values) {
			cnt += ((IntWritable)(value.get(new Text("cnt")))).get();
			sum += ((DoubleWritable)(value.get(new Text("sum")))).get();
		}
		
		//TODO: Emit data for reduce to consume.
		//Create a variable, say cnt_sum_map, of type MapWritable, where you put the cnt and sum computed above.
		//When you put cnt into the map, convert "cnt" to Text (by calling new Text("cnt"))
		//and the value of cnt to IntWritable (by calling new IntWritable(cnt)). 
		//Do similarly for sum. However use DoubleWritable for it. 
		//Then, do context.write and specify the same key provided to combiner and cnt_sum_map as value. 
	}
}

