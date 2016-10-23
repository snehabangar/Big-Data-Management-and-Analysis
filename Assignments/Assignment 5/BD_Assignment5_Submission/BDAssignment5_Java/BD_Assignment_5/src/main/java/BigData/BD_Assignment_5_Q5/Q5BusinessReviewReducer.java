package BigData.BD_Assignment_5_Q5;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q5BusinessReviewReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		    String business = null;
		    String userNRating = null;
		    int size = 0;
		    
			for(Text value:values) {
				if (value.toString().contains("businessdetails###")) {
					business = value.toString();
				} else {
					size++;
				}
			}
		
			if(business !=null) {
					context.write(new Text(key), new Text(String.valueOf(size)));
			}
	}
}
