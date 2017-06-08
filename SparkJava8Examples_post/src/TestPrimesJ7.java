import java.util.ArrayList;
import java.util.List;

//539779
//Time elapsed = 13.915

public class TestPrimesJ7 {
	
	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		long estimatedTime;
		
		int max = 8_000_000;
		
		List<Integer> listInt = new ArrayList<Integer>();
		for(int i=0; i<max; i++)
			listInt.add(i);
		
		int count = 0;
		for(Integer n : listInt) {
			boolean prime = true;
			
			for(int i=2; i<=(int)Math.sqrt(n); i++) {
				if(n % i == 0) {
					prime = false;
					break;
				}
			}
			
			if(prime == true)
				count++;
		}
		
		System.out.println(count);
		
		estimatedTime = System.currentTimeMillis() - startTime;
		System.out.println("Time elapsed = " + estimatedTime / 1000.0);
	}
}
