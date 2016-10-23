package BigData.BD_Assignment_3;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

 public class Q6{
  public static class ReviewMap extends Mapper<LongWritable, Text, Text, DoubleWritable>{
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//from reviews
			String delims = "//^";
			String[] reviewData = StringUtils.split(value.toString(),delims);
			if (reviewData.length ==4) {
				try {
					double rating = Double.parseDouble(reviewData[3]);
					context.write(new Text(reviewData[2]), new DoubleWritable(rating));
				}
				catch (NumberFormatException e) {
					context.write(new Text(reviewData[2]), new DoubleWritable(0.0));
				}
			}		
		}
	
	}

   public static class ReviewReduce extends Reducer<Text,DoubleWritable,Text,Text> {
		
		private Map<String, Double> countMap = new HashMap<String,Double>();
		
		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values,Context context ) throws IOException, InterruptedException {
		
			int count=0;
			double sum = 0.0;
			for (DoubleWritable val : values) {
				sum += val.get();
				count++;
			}
			Double avg =  ((double)sum/(double)count);
			countMap.put(key.toString(), avg);
		}
		@Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            Map<String, Double> sortedMap = sortByValues(countMap);

            int counter = 0;
            for (String key : sortedMap.keySet()) {
                if (counter++ == 10) {
                    break;
                }
                context.write(new Text(key), new Text(sortedMap.get(key).toString()));
            }
        }
	}	
	/*
	   * sorts the map by values. Taken from:
	   * http://javarevisited.blogspot.it/2012/12/how-to-sort-hashmap-java-by-key-and-value.html
	   */
	    private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
	        List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

	        Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

	        	//@Override
	            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
	                return o2.getValue().compareTo(o1.getValue());
	            }
	        });

	        //LinkedHashMap will keep the keys in the order they are inserted
	        //which is currently sorted on natural ordering
	        Collections.reverse(entries);
	        Map<K, V> sortedMap = new LinkedHashMap<K, V>();
	        for (Map.Entry<K, V> entry : entries) {
	            sortedMap.put(entry.getKey(), entry.getValue());
	        }
	        
	        return sortedMap;
	    }

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: Q6 <in> <out>");
			System.exit(2);
		}		
			Job job = Job.getInstance(conf, "Q6");
		job.setJarByClass(Q6.class);
	   
		job.setMapperClass(ReviewMap.class);
		job.setReducerClass(ReviewReduce.class);
		// set output key type 
		job.setOutputKeyClass(Text.class);		
		
		// set output value type
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);
			
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output 
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		//Wait till job completion
		job.waitForCompletion(true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

	
	