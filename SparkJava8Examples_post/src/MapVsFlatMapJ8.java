import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MapVsFlatMapJ8 {

	public static void main(String[] args) throws Exception {
		List<String> words = new ArrayList<String>();
		words.add("java"); words.add("scala"); 
		
		//boxed is needed to convert int's to Integer's in IntStream that w.chars() returns.
	
		Stream<Stream<Integer>> result1  = words.parallelStream().map    ( w -> w.chars().boxed() );
		//[115, 99, 97, 108, 97] [106, 97, 118, 97]
		
		Stream<Integer>         result2  = words.parallelStream().flatMap( w -> w.chars().boxed() );
		//[106, 97, 118, 97, 115, 99, 97, 108, 97]
		
		
		
		result1.forEach( x -> System.out.println( x.collect(Collectors.toList())) );	
		System.out.println( result2.collect(Collectors.toList()) );
	}
}
