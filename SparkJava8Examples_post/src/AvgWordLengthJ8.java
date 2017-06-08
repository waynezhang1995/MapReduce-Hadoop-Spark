import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.IntSummaryStatistics;
import java.util.List;

public class AvgWordLengthJ8 {

	public static void main(String[] args) throws Exception {
		
		String contents = new String(Files.readAllBytes(
				Paths.get("war_and_peace.txt")));

		List<String> words = Arrays.asList(contents.split("[\\P{L}]+"));
		
		long cnt = words.parallelStream().count();	
		
		//Solution 1
		long sum1 = words.parallelStream().map(s->s.length()).reduce(0,(x,y)->x+y);
		System.out.println( 1.0*sum1/cnt );
		
		//Solution 2
		long sum2 = words.parallelStream().map(s->s.length()).mapToInt(Integer::intValue).sum();
		System.out.println( 1.0*sum2/cnt );
		
		//Solution 3
		long sum3 = words.parallelStream().mapToInt(s->s.length()).sum();
		System.out.println( 1.0*sum3/cnt );
		
		//Solution 4
		IntSummaryStatistics summary = words.parallelStream().mapToInt(s->s.length()).summaryStatistics();
		System.out.println( summary );
		
		//count(), sum(), etc, are special cases of reduction.
	}
}
