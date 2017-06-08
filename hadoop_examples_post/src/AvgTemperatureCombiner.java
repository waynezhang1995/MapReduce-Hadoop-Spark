import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgTemperatureCombiner extends Reducer<Text, MapWritable, Text, MapWritable> 
{
	@Override
	public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
		
		int cnt = 0;
		int sum = 0;
		
		for (MapWritable value : values) {
			cnt += ((IntWritable)(value.get(new Text("cnt")))).get();
			sum += ((IntWritable)(value.get(new Text("sum")))).get();
		}
		
		MapWritable cnt_sum_map = new MapWritable();
		cnt_sum_map.put(new Text("cnt"), new IntWritable(cnt));
		cnt_sum_map.put(new Text("sum"), new IntWritable(sum));
		
		context.write(key, cnt_sum_map);
	}
}

