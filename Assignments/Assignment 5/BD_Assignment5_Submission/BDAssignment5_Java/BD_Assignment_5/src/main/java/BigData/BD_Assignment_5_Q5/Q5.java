package BigData.BD_Assignment_5_Q5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Q5 {
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); 
		if (otherArgs.length != 3) {
			System.err.println("Usage: Q5 <business-file> <review-file> <output-file>");
			System.exit(2);
		}
		
		//Job to do reducer side join
		Job job = Job.getInstance(conf, "Q5");
		job.setJarByClass(Q5.class);
		
		
		job.setReducerClass(Q5BusinessReviewReducer.class);

		// set output value type
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class,
				Q5BusiReviewMapper.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]),
				TextInputFormat.class, Q5TXBusinessMapper.class);

		// set output key type
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// set the HDFS path of the input data
	
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		job.waitForCompletion(true);
	}

}
