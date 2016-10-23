package BigData.BD_Assignment_5_Q4;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import BigData.BD_Assignment_5.BusinessMapper;
import BigData.BD_Assignment_5.Q1;
import BigData.BD_Assignment_5.RatingComparator;
import BigData.BD_Assignment_5.ReviewBusinessReducer;
import BigData.BD_Assignment_5.TopReviewMapper;
import BigData.BD_Assignment_5_Q2.UserMapper;
import BigData.BD_Assignment_5.Q1.ReviewMap;
import BigData.BD_Assignment_5.Q1.ReviewReduce;

public class Q4 {
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); 
		if (otherArgs.length != 3) {
			System.err.println("Usage: Q4 <review-file> <user-file> <output-file>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "Q4");
		job.setJarByClass(Q4.class);
		

		job.setMapperClass(ReviewMap.class);
		job.setReducerClass(ReviewReduce.class);

		// set output key type
		job.setOutputKeyClass(Text.class);

		// set output value type
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		Path tempPath = new Path("/asgn5/top10ReviewCount");
		
		FileOutputFormat.setOutputPath(job, tempPath);

		job.waitForCompletion(true);
		// Wait till job completion
		
		
		//Second job to do reducer side join
		Job secondJob = Job.getInstance(conf, "Q4");
		secondJob.setJarByClass(Q4.class);
		
		
		secondJob.setReducerClass(Q4ReviewUserReducer.class);

		// set output value type
		MultipleInputs.addInputPath(secondJob, new Path(otherArgs[1]), TextInputFormat.class,
				UserDetailsMapper.class);
		MultipleInputs.addInputPath(secondJob, tempPath,
				TextInputFormat.class, TopReviewMapper.class);

		// set output key type
		secondJob.setOutputKeyClass(Text.class);
		secondJob.setOutputValueClass(Text.class);

		// set the HDFS path of the input data
	
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(secondJob, new Path(otherArgs[2]));

		secondJob.waitForCompletion(true);
		
//		FileSystem hdfs =FileSystem.get(conf);
//		hdfs.delete(tempPath, true);
		

	}
	
	public static class ReviewMap extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// from business
			String delims = "^";
			String[] businessData = StringUtils.split(value.toString(), delims);

			if (businessData.length == 4) {
					context.write(new Text(businessData[1]), new IntWritable(1));
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
		}
	}
	
	public static class ReviewReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		HashMap<String, Integer> map = new HashMap<String, Integer>();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int size = 0;
			for (IntWritable t : values) {
				size++;
			}
			
			map.put(key.toString(), size);
		}

		@Override
		protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Map<String, Integer> sortedMap = new TreeMap<String, Integer>(new ReviewConuntComparator(map));
			sortedMap.putAll(map);
			
			int i = 0;
			for (Map.Entry<String, Integer> entry : sortedMap.entrySet()) {
				Integer value = entry.getValue();
				context.write(new Text(entry.getKey()),
						new IntWritable(value));
				i++;
				if (i == 10)
					break;
			}

			super.cleanup(context);
		}
		
		
	}

}
