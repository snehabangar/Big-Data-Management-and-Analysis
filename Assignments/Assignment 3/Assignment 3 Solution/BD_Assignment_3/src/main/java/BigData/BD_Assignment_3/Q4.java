package BigData.BD_Assignment_3;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Q4 {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private String tokens = "[|$<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']";

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String cleanLine = value.toString().toLowerCase().replaceAll(tokens, " ");
			String regex = "(?:\\s|\\A)[##]+([A-Za-z0-9-_]+)"; // "(#\\w+)";
			StringTokenizer itr = new StringTokenizer(cleanLine.toString().toLowerCase(), " ");
			while (itr.hasMoreTokens()) {
				String token = itr.nextToken();
				Pattern pattern = Pattern.compile(regex);
				if (token.contains("#")) {
					Matcher matcher = pattern.matcher(token);
					if (matcher.matches()) {
						word.set(token);
						context.write(word, one);
					}
				}
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private Map<Text, IntWritable> countMap = new HashMap<Text, IntWritable>();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			// computes the number of occurrences of a single word
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}

			// puts the number of occurrences of this word into the map.
			// We need to create another Text object because the Text instance
			// we receive is the same for all the words
			countMap.put(new Text(key), new IntWritable(sum));
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

			Map<Text, IntWritable> sortedMap = sortByValues(countMap);

			int counter = 0;
			for (Text key : sortedMap.keySet()) {
				if (counter++ == 10) {
					break;
				}
				context.write(key, sortedMap.get(key));
			}
		}
	}

	/**
	 * The combiner retrieves every word and puts it into a Map: if the word
	 * already exists in the map, increments its value, otherwise sets it to 1.
	 */
	public static class TopNCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			// computes the number of occurrences of a single word
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	/*
	 * sorts the map by values. Taken from:
	 * http://javarevisited.blogspot.it/2012/12/how-to-sort-hashmap-java-by-key-
	 * and-value.html
	 */
	private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
		List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

		Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

			// @Override
			public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
		});

		// LinkedHashMap will keep the keys in the order they are inserted
		// which is currently sorted on natural ordering
		Map<K, V> sortedMap = new LinkedHashMap<K, V>();

		for (Map.Entry<K, V> entry : entries) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}

		return sortedMap;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
		conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
		conf.set("mapreduce.framework.name", "yarn");
		Job job = Job.getInstance(conf, "Q4");
		job.setJarByClass(Q4.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}