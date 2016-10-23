package BigData.BD_Assignment_4;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class Stripes extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(Stripes.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Stripes(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "stripes");
		if (args.length < 4) {
			System.err.println("Usage: Stripes <in> <out> -limit <Word Length Value>");
			System.exit(2);
		}
		for (int i = 0; i < args.length; i += 1) {
			//-skip is used to get the argument -> stop words file to skip stop words
			if ("-skip".equals(args[i])) {
				job.getConfiguration().setBoolean("stripes.skip.patterns", true);
				i += 1;
				job.addCacheFile(new Path(args[i]).toUri());
				// this demonstrates logging
				LOG.info("Added file to the distributed cache: " + args[i]);
			}
			//-limit is used to get the argument -> length of the words to be considered
			if ("-limit".equals(args[i])) {
				i += 1;
				job.getConfiguration().set("stripes.word.length", args[i]);

			}
		}		
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//set mapper class
		job.setMapperClass(StripesMap.class);
		//set mapper output  classes
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(CustomMapWritable.class);

		job.setCombinerClass(StripesReducer.class);
		//set reducer class
		job.setReducerClass(StripesReducer.class);
		//set reducer output classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(CustomMapWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class StripesMap extends Mapper<LongWritable, Text, Text, CustomMapWritable> {
		private final static IntWritable one = new IntWritable(1);
		private String input;
		private Set<String> patternsToSkip = new HashSet<String>();
		private int limitLength;
		private CustomMapWritable occurrenceMap = new CustomMapWritable();
		private Text word = new Text();
		private String tokenPattern = "[|$<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?{}!`\"'#@%&+_]";
		String regex = "\\s*\\b\\s*";

		@Override
		protected void setup(Mapper.Context context) throws IOException, InterruptedException {
			if (context.getInputSplit() instanceof FileSplit) {
				this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
			} else {
				this.input = context.getInputSplit().toString();
			}
			Configuration config = context.getConfiguration();
			if (config.getBoolean("stripes.skip.patterns", false)) {
				URI[] localPaths = context.getCacheFiles();
				parseSkipFile(localPaths[0]);
			}
			limitLength = Integer.parseInt(config.get("stripes.word.length"));
		}

		private void parseSkipFile(URI patternsURI) {
			LOG.info("Added file to the distributed cache: " + patternsURI);
			try {
				BufferedReader fis = new BufferedReader(new FileReader(new File(patternsURI.getPath()).getName()));
				String pattern;
				while ((pattern = fis.readLine()) != null) {
					patternsToSkip.add(pattern);
				}
			} catch (IOException ioe) {
				System.err.println("Caught exception while parsing the cached file '" + patternsURI + "' : "
						+ StringUtils.stringifyException(ioe));
			}
		}

		@Override
		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			//set the number of neighbours
			int neighbors = context.getConfiguration().getInt("neighbors", 7);
			
			//remove all the symbols
			String cleanLine = lineText.toString().toLowerCase().replaceAll(tokenPattern, " ");
			String[] tokens = cleanLine.toString().split("\\s+");
						
			if (tokens.length > 1) {
				for (int i = 0; i < tokens.length; i++) {
					//skip the token if it is empty or a stop word or has length greater than the set limit
					if (tokens[i].isEmpty() || patternsToSkip.contains(tokens[i]) || tokens[i].length() != limitLength ) {
						continue;
					}
					word.set(tokens[i]);
					occurrenceMap.clear();
					int current = i;
					int start = current;
					int startCount = 0;
					
					//while you get neighbors equal to the neighbor's count or you reach start of the token array
					while (startCount < neighbors && current - start < current) {
						
						start--;						
						//skip the token if it is empty or a stop word or has length greater than the set limit
						if (tokens[start].isEmpty() || patternsToSkip.contains(tokens[start])|| tokens[start].length() != limitLength)
							continue;
						Text neighbor = new Text(tokens[start]);

						if (occurrenceMap.containsKey(neighbor)) {
							//add the neighbor to occurrence map and increment the count
							IntWritable count = (IntWritable) occurrenceMap.get(neighbor);
							count.set(count.get() + 1);
							occurrenceMap.put(neighbor, count);
						} else {
							//add the neighbor to occurrence map with count 1
							occurrenceMap.put(neighbor, one);
						}
						startCount++;
					}
					int end = 0;
					int endCount = 0;
					//while you get neighbors equal to the neighbor's count or you reach end of token array
					while (endCount < neighbors && current + end < tokens.length - 1) {
						end++;
						if (tokens[end].isEmpty() || patternsToSkip.contains(tokens[current + end])
								|| tokens[current + end].length() != limitLength )
							continue;
						Text neighbor = new Text(tokens[current + end]);
						endCount++;
						if (occurrenceMap.containsKey(neighbor)) {
							//add the neighbor to occurrence map and increment the count
							IntWritable count = (IntWritable) occurrenceMap.get(neighbor);
							count.set(count.get() + 1);
							occurrenceMap.put(neighbor, count);
						} else {
							//add the neighbor to occurrence map with count 1
							occurrenceMap.put(neighbor, new IntWritable(1));
						}
					}
					if(!occurrenceMap.isEmpty())
					context.write(word, occurrenceMap);
				}
			}
		}
	}

	public static class StripesReducer extends Reducer<Text, CustomMapWritable, Text, CustomMapWritable> {
		private CustomMapWritable cooccurenceMap = new CustomMapWritable();

		@Override
		protected void reduce(Text key, Iterable<CustomMapWritable> values, Context context)
				throws IOException, InterruptedException {
			cooccurenceMap.clear();
			for (MapWritable value : values) {
				addAll(value);
			}
			//Output format -  hadoop {big : 1 , data : 4 , test : 2}
			context.write(key, cooccurenceMap);
		}
		private void addAll(MapWritable mapWritable) {
			Set<Writable> keys = mapWritable.keySet();
			for (Writable key : keys) {
				IntWritable valCount = (IntWritable) mapWritable.get(key);
				if (cooccurenceMap.containsKey(key)) {
					IntWritable count = (IntWritable) cooccurenceMap.get(key);
					count.set(count.get() + valCount.get());
					cooccurenceMap.put(key, count);
				} else {
					cooccurenceMap.put(key, valCount);
				}
			}
		}
	}
}

class CustomMapWritable extends MapWritable {
	@Override
	public String toString() {
		
		Set<Writable> keys = this.keySet();
		String result = "{";
		for (Writable k : keys) {
			IntWritable val = (IntWritable) this.get(k);
			result = result + " "+ k.toString() + ":" + val.toString() + " ,";
		}
		result =  result.replaceFirst(".$","");
		result = result + "}";
		return result;

	}

}
