package BigData.BD_Assignment_3;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q2 {

	public static class BusinessMap extends Mapper<LongWritable, Text, Text, IntWritable> {
		Set<String> outputSet = new HashSet<String>();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// from business
			String delims = "^";
			String[] businessData = StringUtils.split(value.toString(), delims);

			if (businessData.length == 3) {
				if (businessData[1].contains("NY") && businessData[2].contains("Restaurants")) {
					String outputKey = businessData[0].toString() + " " + businessData[1].toString();
					if (!outputSet.contains(outputKey)) {
						outputSet.add(outputKey);
						context.write(new Text(businessData[0] + " " + businessData[1]), null);
					}
				}
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int count = 0;
			for (IntWritable t : values) {
				count++;
			}

			context.write(key, new IntWritable(count));

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); // get all args

		if (otherArgs.length != 2) {
			System.err.println("Usage: Q2 <in> <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "Q2");
		job.setJarByClass(Q2.class);

		job.setMapperClass(BusinessMap.class);
		job.setNumReduceTasks(0);
		// job.setReducerClass(Reduce.class); //new comment

		// set output key type

		job.setOutputKeyClass(Text.class);

		// set output value type
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
