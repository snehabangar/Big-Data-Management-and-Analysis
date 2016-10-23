package BigData.BD_Assignment_5;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReviewBusinessReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		    String rating = null;
		    String business = null;
			for(Text value:values) {
				if (value.toString().contains("rating##")) {
					rating = value.toString();
				} else {
					business = value.toString();
				}
			}
		
			if(rating !=null) {
				String[] ratingData = rating.split("\t");
				String result = business + "\t" + ratingData[1];
				context.write(key, new Text(result));
			}
			
			
	}
	
	

}
