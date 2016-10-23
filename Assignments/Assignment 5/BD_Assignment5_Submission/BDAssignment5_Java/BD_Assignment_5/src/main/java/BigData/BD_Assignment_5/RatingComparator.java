package BigData.BD_Assignment_5;

import java.util.Comparator;
import java.util.Map;

public class RatingComparator implements Comparator<Object>{
	
	Map<String, Float> map;
	 
	public RatingComparator(Map<String, Float> map) {
		this.map = map;
	}
		
	public int compare(Object obj1, Object obj2) {
		
		Float rating1 = (Float) map.get(obj1);
		Float rating2 = (Float) map.get(obj2);
		
		int compare=rating2.compareTo(rating1);
		
		if(compare==0)
			return 1;		
		return compare;
	}

}
