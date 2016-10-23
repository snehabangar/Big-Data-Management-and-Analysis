package BigData.BD_Assignment_5_Q4;

import java.util.Comparator;
import java.util.Map;

public class ReviewConuntComparator implements Comparator<Object>{
	
	Map<String, Integer> map;
	 
	public ReviewConuntComparator(Map<String, Integer> map) {
		this.map = map;
	}
		
	public int compare(Object obj1, Object obj2) {
		
		Integer rating1 = (Integer) map.get(obj1);
		Integer rating2 = (Integer) map.get(obj2);
		
		int compare=rating2.compareTo(rating1);
		
		if(compare==0)
			return 1;		
		return compare;
	}

}
