import java.net.URI;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

/*This class is responsible for running map reduce job*/
//Change a path as indicated below. 
public class MovielensGenreAvg extends Configured implements Tool
{
	
	public int run(String[] args) throws Exception {
		BasicConfigurator.configure();
		//Logger.getRootLogger().setLevel(Level.ERROR);
		
		args = new String[] {"input_movielens", "output_movielens"};
		
		if(args.length !=2) {
			System.err.println("Usage: MovielensAvg <input path> <output path>");
			System.exit(-1);
		}
		
		System.out.println("See output in folder: " + args[1]);
		
		//remove output directory if already there
		FileSystem fs = FileSystem.get(getConf());
		fs.delete(new Path(args[1]), true); // delete file, true for recursive
		
		Job job = Job.getInstance(getConf()); 
		job.setJarByClass(MovielensGenreAvg.class);
		job.setJobName("Movielens Genre Avg");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.setInputDirRecursive(job, true); //To read recursively all sub-directories, not set by default
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		job.setMapperClass(MovielensGenreAvgMapper.class);
		job.setReducerClass(MovielensGenreAvgReducer.class);
		job.setCombinerClass(MovielensGenreAvgCombiner.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		//We assume that the movies.csv file fits in memory.
		//This call will save the file locally in each machine in the cluster. 
		//CHANGE THIS PATH TO YOUR SPECIFIC LOCATION
		job.addCacheFile(new URI("file:///C:/Users/Alex/workspace_neon/hadoop_examples/movielens/ml-latest-small/movies.csv"));

		long startTime = System.currentTimeMillis();
		
		boolean success = job.waitForCompletion(true);
		
		System.out.println("Time elapsed (sec) = " + (System.currentTimeMillis() - startTime) / 1000.0);
		
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		MovielensGenreAvg driver = new MovielensGenreAvg();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
}