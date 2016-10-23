README.TXT
------------------------
This file lists the instructions to run the project TweetMapReduce.

Part 1 -> TweetDownload file uses twitter4j API to search for tweets on the given topic.
Topic used in this program is "India".Timeline -> 1 day approximately.
File size - 1.5 to 2 MB
It then creates a file with current timestamp on server and writes all the tweets in it.
Part 2 -> TweetWordCount file takes the txt files generated in Part 1 as input and runs a map reduce job on it.The output file contains all the hashtags and their corresponding count of occurence.    

----Author----
Sneha Bangar 

---Library used---
twitter4j API is used to search and download tweets.
JAR details - twitter4j-core-4.0.4.jar
The API has limit of 100 tweets in a query.I have used looping structures to get more no of tweets.

----Class Details----

1. TweetDownload.Java
   This is the main class which calls function getTweets to search and download tweets and then save them in a file on HDFS server.
   
2.TweetWordCount.Java
   This is the calss that contains functions map and reduce to count the no of different hashtags from the tweets.

----Run in Command Line----
Command line run format to run on hdfs -
My maven project creates two packaged jars ,one is TweetMapReduce-0.0.1-SNAPSHOT-jar-with-dependencies.jar which includes the twitter4j API jar and two TweetMapReduce-0.0.1-SNAPSHOT.jar whcih does not include the twitter4j dependency jar.To use second jar you need to add twitter4j jar in hadoop classpath.Please find below the commands that explain how to run the project.

#Run TweetDownload
1)Using the project jar with all dependencies included - 
hadoop jar TweetMapReduce-0.0.1-SNAPSHOT-jar-with-dependencies.jar  <destination-folder>

for example-
hadoop jar TweetMapReduce-0.0.1-SNAPSHOT-jar-with-dependencies.jar hdfs://cshadoop1/user/ssb151030/Tweets/
  
2)Using project jar withouth any included dependencies.
hadoop jar TweetMapReduce-0.0.1-SNAPSHOT.jar BigData.TweetMapReduce.TweetDownload <destination-folder> -libjar twitter4j-core-4.0.4.jar

for example-
hadoop jar TweetMapReduce-0.0.1-SNAPSHOT.jar BigData.TweetMapReduce.TweetDownload hdfs://cshadoop1/user/ssb151030/Tweets/ -libjar twitter4j-core-4.0.4.jar

#Run TweetWordCount
2)Using project jar withouth any included dependencies.
hadoop jar TweetMapReduce-0.0.1-SNAPSHOT.jar BigData.TweetMapReduce.TweetWordCount <Folder-containing tweets txt files> <output folder>

for example-
hadoop jar TweetMapReduce-0.0.1-SNAPSHOT.jar BigData.TweetMapReduce.TweetWordCount hdfs://cshadoop1/user/ssb151030/Tweets/ hdfs://cshadoop1/user/ssb151030/tweetsOp

---Results----

1) After you run the program TweetDownload6 a txt file conataining tweets will be saved on server.
File name Format -> tweets-dd-mm-yy-hh-mm-ss.txt for example ,tweets-31-01-16-02-13-40.txt

2) After you run the program TweetWordCount the output of map reduce will be saved in the output folder you passed as argument while running the program.

File name Format -> tweets-dd-mm-yy-hh-mm-ss.txt for example ,tweets-31-01-16-02-13-40.txt


Use  hdfs dfs -cat <filename>.<extension> command to see the any extracted file on server.
