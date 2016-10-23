package BigData.BD_Assignment_5_Q2;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	String username;

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		Configuration conf = context.getConfiguration();
		username = conf.get("username");
	}



	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String delims = "^";
		String[] businessData = StringUtils.split(value.toString(), delims);

		if (businessData.length == 3) {
			    if(businessData[1].equalsIgnoreCase(username)) {
			    	context.write(new Text(businessData[0]), new Text("userdetails###\t" + businessData[1]));
			    }
		}
	}
}
