package BigData.BD_Assignment_5_Q2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q2 {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); 
		if (otherArgs.length != 4) {
			System.err.println("Usage: Q2 <user-file> <review-file> <person-name> <output-file>");
			System.exit(2);
		}
		
		//Job to do reducer side join
		conf.set("username", otherArgs[2]);
		Job job = Job.getInstance(conf, "Q2");
		job.setJarByClass(Q2.class);
		
		
		job.setReducerClass(ReviewUserReducer.class);

		// set output value type
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class,
				UserMapper.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
				TextInputFormat.class, ReviewMapper.class);

		// set output key type
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// set the HDFS path of the input data
	
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

		job.waitForCompletion(true);
	}

}
