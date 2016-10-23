package BigData.BD_Assignment_5_Q3;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class UserReviewMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// from business
		String delims = "^";
		String[] businessData = StringUtils.split(value.toString(), delims);

		if (businessData.length == 4) {
			    String userNRating = businessData[1] + "\t" + businessData[3];
				context.write(new Text(businessData[2]), new Text(userNRating));
		}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	}
}
