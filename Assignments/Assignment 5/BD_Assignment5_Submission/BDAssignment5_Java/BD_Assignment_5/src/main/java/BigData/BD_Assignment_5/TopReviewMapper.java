package BigData.BD_Assignment_5;


import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopReviewMapper extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String delims = "\t";
		String[] businessData = StringUtils.split(value.toString(), delims);

		if (businessData.length == 2) {
				context.write(new Text(businessData[0]), new Text("rating##\t" + businessData[1]));
		}
	}
	
	

}
