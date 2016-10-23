package BigData.BD_Assignment_5_Q3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import BigData.BD_Assignment_5_Q2.Q2;
import BigData.BD_Assignment_5_Q2.ReviewMapper;
import BigData.BD_Assignment_5_Q2.ReviewUserReducer;
import BigData.BD_Assignment_5_Q2.UserMapper;

public class Q3 {
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); 
		if (otherArgs.length != 3) {
			System.err.println("Usage: Q2 <user-file> <review-file> <output-file>");
			System.exit(2);
		}
		
		//Job to do reducer side join
		Job job = Job.getInstance(conf, "Q3");
		job.setJarByClass(Q3.class);
		
		
		job.setReducerClass(StanfordBusiReducer.class);

		// set output value type
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class,
				UserReviewMapper.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]),
				TextInputFormat.class, StanfordBusinessMapper.class);

		// set output key type
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// set the HDFS path of the input data
	
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		job.waitForCompletion(true);
	}

}
