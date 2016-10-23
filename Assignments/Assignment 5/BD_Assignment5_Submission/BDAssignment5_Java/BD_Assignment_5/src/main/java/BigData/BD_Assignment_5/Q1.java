package BigData.BD_Assignment_5;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;




public class Q1 {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); 
		if (otherArgs.length != 3) {
			System.err.println("Usage: Q1 <input-file1> <input-file2> <output-file>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "Q1");
		job.setJarByClass(Q1.class);
		

		job.setMapperClass(ReviewMap.class);
		job.setReducerClass(ReviewReduce.class);

		// set output key type
		job.setOutputKeyClass(Text.class);

		// set output value type
		job.setMapOutputValueClass(FloatWritable.class);
		job.setOutputValueClass(IntWritable.class);

		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		Path tempPath = new Path("/asgn5/top10review");
		
		FileOutputFormat.setOutputPath(job, tempPath);

		job.waitForCompletion(true);
		// Wait till job completion
		
		
		//Second job to do reducer side join
		Job secondJob = Job.getInstance(conf, "Q1");
		secondJob.setJarByClass(Q1.class);
		
		
		secondJob.setReducerClass(ReviewBusinessReducer.class);

		// set output value type
		MultipleInputs.addInputPath(secondJob, new Path(otherArgs[1]), TextInputFormat.class,
				BusinessMapper.class);
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
	
	public static class ReviewMap extends Mapper<LongWritable, Text, Text, FloatWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// from business
			String delims = "^";
			String[] businessData = StringUtils.split(value.toString(), delims);

			if (businessData.length == 4) {
					context.write(new Text(businessData[2]), new FloatWritable(Float.valueOf(businessData[3])));
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
		}
	}
	
	public static class ReviewReduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {
		
		HashMap<String, Float> map = new HashMap<String, Float>();

		public void reduce(Text key, Iterable<FloatWritable> values, Context context)
				throws IOException, InterruptedException {

			Float count = (float)0;
			int size = 0;
			for (FloatWritable t : values) {
				count =+ t.get();
				size++;
			}
			Float average = count/size;
			//DecimalFormat df = new DecimalFormat("#.##");
			//average = Float.valueOf(df.format(average));
			map.put(key.toString(), average);
		}

		@Override
		protected void cleanup(Reducer<Text, FloatWritable, Text, FloatWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Map<String, Float> sortedMap = new TreeMap<String, Float>(new RatingComparator(map));
			sortedMap.putAll(map);
			
			int i = 0;
			for (Map.Entry<String, Float> entry : sortedMap.entrySet()) {
				DecimalFormat df = new DecimalFormat("#.##");
				Float value = Float.valueOf(df.format(entry.getValue()));
				context.write(new Text(entry.getKey()),
						new FloatWritable(value));
				i++;
				if (i == 10)
					break;
			}

			super.cleanup(context);
		}
		
		
	}

}
