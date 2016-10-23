package BigData.BD_Assignment_5_Q3;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StanfordBusiReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		    String address = null;
		    String userNRating = null;
		    ArrayList<String> reviewList = new ArrayList<String>();
		    
			for(Text value:values) {
				if (value.toString().contains("businessdetails###")) {
					address = value.toString();
				} else {
					reviewList.add(value.toString());
				}
			}
		
			if(address !=null) {
				for(int i=0; i< reviewList.size(); i++) {
					userNRating = reviewList.get(i);
					String result[] = userNRating.split("\t");
					context.write(new Text(result[0]), new Text(result[1]));
				}
			}
	}
}
