package BigData.BD_Assignment_5_Q2;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class ReviewMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// from business
		String delims = "^";
		String[] businessData = StringUtils.split(value.toString(), delims);

		if (businessData.length == 4) {
				context.write(new Text(businessData[1]), new Text(businessData[3]));
		}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	}
}
