import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgTemperatureReducer extends Reducer<Text, MapWritable, Text, DoubleWritable> 
{
	@Override
	public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
		
		int cnt = 0;
		int sum = 0;
		
		for (MapWritable value : values) {
			cnt += ((IntWritable)(value.get(new Text("cnt")))).get();
			sum += ((IntWritable)(value.get(new Text("sum")))).get();
		}
				
		context.write(key, new DoubleWritable(1.0*sum/cnt) );
	}
}

