import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovielensGenreAvgMapper extends Mapper<LongWritable, Text, Text, MapWritable> 
{
	Map<String, String[]> movieGenres = new HashMap<>();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	    if (context.getCacheFiles() != null
	            && context.getCacheFiles().length > 0) {

	    	URI[] localPath = context.getCacheFiles();
	    	URI movieURI = localPath[0]; 
	    	System.out.println(movieURI);
	    	
	        BufferedReader in = new BufferedReader(new FileReader(new File(movieURI)));
	        String line = in.readLine(); //read header
	        while ((line = in.readLine()) != null) {
	        	//The following splits the line using "," as separator. 
	        	//The regular expression used is more complex than just "," 
	        	//because we want to ignore commas in the title of the movies. 
	        	//Titles with commas are surrounded by quotes. 
	        	String[] lineArr = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
	        	String movieID = lineArr[0];
	        	String[] genres = lineArr[2].split("\\|");

	        	movieGenres.put(movieID, genres);
	        }
	        in.close();
	    }
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String line = value.toString();
		
		//check to see if this is the header line
		if(line.contains("userId"))
			return;
		
		String[] lineArr = line.split(",");
		//String userID = lineArr[0];
		String movieID = lineArr[1];
		double rating = Double.parseDouble(lineArr[2]);
		
		String[] genres = movieGenres.get(movieID); 
		
		for(String genre : genres) {
			//TODO: emit a K,V pair for each genre. 
			//K is genre
			//V is a MapWritable containing rating and the number 1 (similar to MovielensAvg)
		}
	}
}
