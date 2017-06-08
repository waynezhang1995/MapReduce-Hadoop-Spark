import java.util.ArrayList;
import java.util.List;

//serial stream
//539779
//Time elapsed = 12.293

//parallel stream
//539779
//Time elapsed = 4.934

public class TestPrimesJ8 {
	
	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		long estimatedTime;
		
		int max = 8000000;
		
		List<Integer> listInt = new ArrayList<Integer>();
		for(int i=0; i<max; i++)
			listInt.add(i);
		
		System.out.println(
				listInt.parallelStream()
						.filter(
							n -> {
								for(int i=2; i<=(int)Math.sqrt(n); i++)
									if(n % i == 0)
										return false;
								return true;
							}
						).count()
				);
		
		estimatedTime = System.currentTimeMillis() - startTime;
		System.out.println("Time elapsed = " + estimatedTime / 1000.0);
	}
}
