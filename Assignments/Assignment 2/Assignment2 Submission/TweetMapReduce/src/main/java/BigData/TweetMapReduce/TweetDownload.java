package BigData.TweetMapReduce;

/**
 * TweetDownload
 * Author: Sneha Bangar
 *
 */
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;

public class TweetDownload {
	public final List<String> srcUrlLst = new ArrayList<String>();
	public String topic = "India";
	public static void main(String[] args) {
		if (null == args || args.length <= 0) {
			System.out.println("Please enter Destination folder to upload the files!");
			return;
		}
		String dstPath = args[0];
		if (!dstPath.endsWith("/"))
			dstPath = dstPath + "/";
		TweetDownload tweetDwnld = new TweetDownload();
		tweetDwnld.getTweets(dstPath);
	}

	public void getTweets(String dstPath) {

		OutputStream out = null;
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));
		
		try {
			FileSystem fs = FileSystem.get(conf);
			SimpleDateFormat sd = new SimpleDateFormat("dd-M-yyyy-hh-mm-ss");
			Date curDate = new Date();
			String destUri = dstPath + "tweets-" + sd.format(curDate) + ".txt";
			//System.out.println("dest URI:" + destUri);

			Path outPath = new Path(destUri);
			out = fs.create(outPath, true);

			BufferedWriter br = new BufferedWriter(new OutputStreamWriter(out));

			TwitterFactory factory = new TwitterFactory();
			
			AccessToken accessToken = new
			AccessToken("4837884648-cOG3nuBLE3G5yYH9OI7EE5at782O9e5mafMI7uv","UUqcsezmULOMirtbN0MCo9XlodosNNaT6Kk84PFSl2ycQ");
			Twitter twitter = factory.getInstance();
			twitter.setOAuthConsumer("8nDEo9fQuSckqSpNhv3Zb5FBX",
			  "aMmaxryLiE6IV0rw1nhRQanV9eYqX1cQwsDo8tZakOrkyfrLLQ");
			twitter.setOAuthAccessToken(accessToken);
			 
			QueryResult result = null;
			long lowerTweetId = 0;
			for (int i = 0; i < 10; i++) {
				Query query = new Query(topic);
				if (i != 0) {
					query.setMaxId(lowerTweetId - 1);
				}
				query.setCount(100);
				do {
					result = twitter.search(query);
					List<Status> tweets = result.getTweets();
					for (Status tweet : tweets) {
						lowerTweetId = tweet.getId();
						br.write(tweet.getText());
					}
				} while ((query = result.nextQuery()) != null);
			}
			System.out.println("Tweets File written successfully written on HDFS server");
			br.close();
			System.exit(0);

		} catch (TwitterException te) {
			te.printStackTrace();
			System.out.println("Failed to search tweets: " + te.getMessage());
			System.exit(-1);
		} catch (IOException ie) {
			ie.printStackTrace();
			System.out.println("IO Exception: " + ie.getMessage());
			System.exit(-1);

		}

	}

}
