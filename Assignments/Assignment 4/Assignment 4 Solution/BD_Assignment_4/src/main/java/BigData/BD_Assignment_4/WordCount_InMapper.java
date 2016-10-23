package BigData.BD_Assignment_4;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount_InMapper {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private Map<String, Integer> tokenMap;
		
		protected void setup(Context context) throws IOException, InterruptedException {
			//associative array to maintain state across multiple map calls
			tokenMap = new HashMap<String, Integer>();
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer sTokenizer = new StringTokenizer(value.toString());
			while (sTokenizer.hasMoreElements()) {
				String token = sTokenizer.nextToken();
				Integer count = tokenMap.get(token);
				if (count == null)
					count = new Integer(0);
				count += 1;
				tokenMap.put(token, count);
			}
		}
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			IntWritable iwCount = new IntWritable();
			Text text = new Text();
			Set<String> keys = tokenMap.keySet();
			for (String k : keys) {
				text.set(k);
				iwCount.set(tokenMap.get(k));
				context.write(text, iwCount);
			}
		}
	}


  public static class InMapperReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
    conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
    conf.set("mapreduce.framework.name", "yarn");
    Job job = Job.getInstance(conf, "InMapperWordCount");
    job.setJarByClass(WordCount_InMapper.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(InMapperReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}