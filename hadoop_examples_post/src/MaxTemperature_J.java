import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Collection;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.FileUtils;

public class MaxTemperature_J {
	
	private static final int MISSING = 9999;
	
	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();
		
		//args = new String[] {"C:/Users/Alex/workspace/myhadoop_examples/weather_data"};
		
		if(args.length !=1) {
			System.err.println("Usage: MaxTemperaturePureJava <input path>");
			System.exit(-1);
		}
		
		Collection<File> files = FileUtils.listFiles(
				new File(args[0]), null, true);
		
		TreeMap<String, Integer> maxValueMap = new TreeMap<>();
		
		for(File f : files) {
			InputStream fileStream = new FileInputStream(f);
			InputStream gzipStream = new GZIPInputStream(fileStream);
			Reader decoder = new InputStreamReader(gzipStream);
			BufferedReader buffered = new BufferedReader(decoder);
			
			String value;

			while ((value = buffered.readLine()) != null) {

				String line = value.toString();
				String year = line.substring(15, 19);
				
				if( !maxValueMap.containsKey(year) ) 
					maxValueMap.put(year, Integer.MIN_VALUE);
				
				int airTemperature;
				
				if (line.charAt(87) == '+') { // parseInt doesn't like leading plus
					// signs
					airTemperature = Integer.parseInt(line.substring(88, 92));
				} else {
					airTemperature = Integer.parseInt(line.substring(87, 92));
				}
				
				String quality = line.substring(92, 93);
				if (airTemperature != MISSING && quality.matches("[01459]")) {
					if(airTemperature > maxValueMap.get(year)) {
						maxValueMap.put(year, airTemperature);
					}
				}
			}
			
			buffered.close();
		}
		
		System.out.println(maxValueMap);
		
		System.out.println("Time elapsed (sec) = " + (System.currentTimeMillis() - startTime) / 1000.0);
	}

}
