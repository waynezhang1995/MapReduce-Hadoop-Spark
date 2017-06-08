import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovielensAvgMapper extends Mapper<LongWritable, Text, Text, MapWritable> 
{
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String line = value.toString();
		
		//check to see if this is the header line
		if(line.contains("userId"))
			return;
		
		String[] lineArr = line.split(",");
		String userID = lineArr[0];
		String movieID = lineArr[1];
		double rating = Double.parseDouble(lineArr[2]);
		
		//We want to compute both the sum and count of ratings, 
		//so that we can compute the average of ratings. 
		//We emit the rating and the number 1. 
		//The latter will be used for counting.
		MapWritable cnt_sum_map = new MapWritable();
		cnt_sum_map.put( new Text("cnt"), new IntWritable(1) );
		cnt_sum_map.put( new Text("sum"), new DoubleWritable(rating) );
		
		context.write(new Text("m"+movieID), cnt_sum_map);
		
		//TODO: Add a line to emit userID and cnt_sum_map. 
		//The line to be added is very similar to the above. 
		//You just need to prefix the userID by "u"
	}
}
