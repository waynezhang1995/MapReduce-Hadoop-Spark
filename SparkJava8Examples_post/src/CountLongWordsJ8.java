import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.Stream;

public class CountLongWordsJ8 {

	public static void main(String[] args) throws Exception {
		
		String contents = new String(Files.readAllBytes(Paths.get("war_and_peace.txt")));

		Stream<String> words = Arrays.stream(contents.split("[\\P{L}]+")).parallel();
		
		System.out.println( words.filter(w -> w.length() > 12).count() );
	}
}
