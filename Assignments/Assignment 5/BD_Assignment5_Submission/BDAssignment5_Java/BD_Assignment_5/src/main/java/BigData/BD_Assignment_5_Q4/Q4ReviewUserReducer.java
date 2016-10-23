package BigData.BD_Assignment_5_Q4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q4ReviewUserReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		    String reviewDetails = null;
		    String userDetails = null;
			for(Text value:values) {
				if (value.toString().contains("rating##")) {
					reviewDetails = value.toString();
				} else {
					userDetails = value.toString();
				}
			}
		
			if(reviewDetails !=null) {
				
				context.write(key, new Text(userDetails));
			}
			
			
	}
}
