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
		for (MapWritable value : values) {

			cnt += ((IntWritable)(value.get(new Text("cnt")))).get();
			sum += ((DoubleWritable)(value.get(new Text("sum")))).get();
		}
		context.write(key, new DoubleWritable(1.0*sum/cnt) );
		//TODO: iterate over the values, extract "cnt" and "sum" and accumulate them into cnt and sum.
		//Then emit the result, sum/cnt, using the same key we got in reduce().
		//Convert sum/cnt to DoubleWritable (by doing new DoubleWritable(sum/cnt)).

	}
}

