import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

public class LargestWordsJ8 {

	public static void main(String[] args) throws Exception {
		
		String contents = new String(Files.readAllBytes(
				Paths.get("war_and_peace.txt")));
		
		Stream<String> words = Arrays.stream(contents.split("[\\P{L}]+")).parallel();
		
		Optional<String> largest = words.max( (s,t) -> Integer.compare(s.length(), t.length()) );
		
		if (largest.isPresent()) {
			String lw = largest.get();
			System.out.println("largest: " + lw);
		    
			//how many times this word shows up
			System.out.println( words.filter( s->s.equals(lw) ).count() );
		}
	}	
}
