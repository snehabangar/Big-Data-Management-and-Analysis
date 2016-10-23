package BigData.BD_Assignment_5_Q4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReviewUserReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		    String userdetails = null;
		    String reviewdetails = null;
			for(Text value:values) {
				if (value.toString().contains("rating##")) {
					reviewdetails = value.toString();
				} else {
					userdetails = value.toString();
				}
			}
		
			if(reviewdetails !=null) {
				String[] ratingData = userdetails.split("\t");
				String result = ratingData[1];
				context.write(key, new Text(result));
			}
			
			
	}
	
}
