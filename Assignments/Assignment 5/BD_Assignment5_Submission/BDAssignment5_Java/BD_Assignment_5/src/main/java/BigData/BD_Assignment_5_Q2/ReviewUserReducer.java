package BigData.BD_Assignment_5_Q2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReviewUserReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		    String username = null;
		    float totalRating = 0;
		    int size = 0;
			for(Text value:values) {
				if (value.toString().contains("userdetails###")) {
					username = value.toString();
				} else {
					float currentrating = Float.valueOf(value.toString());
					totalRating += currentrating;
					size++;
				}
			}
		
			if(username !=null) {
				String[] userData = username.split("\t");
				float average = 0;
				if(size > 0)
				    average = totalRating/size;
				
				String result = userData[1] + "\t" + average;
				context.write(key, new Text(result));
			}
	}						
}
	
