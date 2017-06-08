import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

/*This class is responsible for running map reduce job*/
public class MaxTemperature extends Configured implements Tool
{
	
	public int run(String[] args) throws Exception {
		BasicConfigurator.configure();
		//Logger.getRootLogger().setLevel(Level.ERROR);
		
		//args = new String[] {"weather_data/ftp.ncdc.noaa.gov/pub/data/noaa/1930", "output"};
		//args = new String[] {"weather_data_2", "output"};
		args = new String[] {"input_weather", "output_weather"};
		
		if(args.length !=2) {
			System.err.println("Usage: MaxTemperatureDriver <input path> <output path>");
			System.exit(-1);
		}
		
		System.out.println("See output in folder: " + args[1]);
		
		//remove output directory if already there
		FileSystem fs = FileSystem.get(getConf());
		fs.delete(new Path(args[1]), true); // delete file, true for recursive
		
		Job job = Job.getInstance(getConf()); 
		job.setJarByClass(MaxTemperature.class);
		job.setJobName("Max Temperature");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.setInputDirRecursive(job, true); //To read recursively all sub-directories, not set by default
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setReducerClass(MaxTemperatureReducer.class);
		job.setCombinerClass(MaxTemperatureReducer.class);
		
//		The setOutputKeyClass() and setOutputValueClass() methods control the output
//		types for the reduce function, and must match what the Reduce class produces. The map
//		output types default to the same types, so they do not need to be set if the mapper
//		produces the same types as the reducer (as it does in our case). However, if they are
//		different, the map output types must be set using the setMapOutputKeyClass() and
//		setMapOutputValueClass() methods.
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		long startTime = System.currentTimeMillis();
		
		boolean success = job.waitForCompletion(true);
		
		System.out.println("Time elapsed (sec) = " + (System.currentTimeMillis() - startTime) / 1000.0);
		
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		MaxTemperature driver = new MaxTemperature();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
}