package BigData.BD_Assignment_5_Q5;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Q5TXBusinessMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	String state = ", TX";
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String delims = "^";
		String[] businessData = StringUtils.split(value.toString(), delims);

		if (businessData.length == 3) {
			    if(businessData[1].contains((state))) {
			    	context.write(new Text(businessData[0]), new Text("businessdetails###\t" + businessData[1]));
			    }
		}
	}
}