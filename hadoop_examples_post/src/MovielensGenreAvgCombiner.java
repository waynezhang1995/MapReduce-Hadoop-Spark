import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovielensGenreAvgCombiner extends Reducer<Text, MapWritable, Text, MapWritable>
{
	@Override
	public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {

		int cnt = 0;
		double sum = 0;

		//TODO: the rest of the method.
		//Very similar to MovielensAvgCombiner
		for (MapWritable value : values) {
			cnt += ((IntWritable)(value.get(new Text("cnt")))).get();
			sum += ((DoubleWritable)(value.get(new Text("sum")))).get();
		}
		MapWritable cnt_sum_map = new MapWritable();
		cnt_sum_map.put(new Text("cnt"), new IntWritable(cnt));
		cnt_sum_map.put(new Text("sum"), new DoubleWritable(sum));

		context.write(key, cnt_sum_map);
	}
}

