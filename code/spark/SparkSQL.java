package code.spark;

import code.a4example.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zahraiman on 11/19/15.
 */
public class SparkSQL {
    public static String outputPath = "data/";
    public static String inputPath = "data/tweets2009-06.txt";
    public static final boolean readFromParquet = true;

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("JavaBadRecordCount").setMaster("local[2]");
        Configuration hadoopConf = new Configuration();
        // blank line is used as delimeter
        // Note that the first record have have an extra line "total number:#" as its first line
        hadoopConf.set("textinputformat.record.delimiter", "\n\n");
        // If the submission is on EC2, you need to set access key and secret access key
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(javaSparkContext);

        DataFrame tweetData;

        if(!readFromParquet) {

            //load data
            JavaPairRDD<LongWritable, Text> lines = javaSparkContext.newAPIHadoopFile(inputPath, TextInputFormat.class, LongWritable.class, Text.class, hadoopConf);
            System.out.println("COUNT1: " + lines.count());
            // Filter the data and keep the tweets that have at least one Hashtag and one mention sign
            JavaRDD<Row> filteredTweets = lines.values().filter(new Function<Text, Boolean>() {
                @Override
                public Boolean call(Text v1) throws Exception {
                    String tweetInf = v1.toString();
                    String tweetText = "";
                    if (tweetInf.startsWith("total"))
                        tweetText = tweetInf.split("\n")[3];
                    else
                        tweetText = tweetInf.split("\n")[2];
                    return (tweetText.contains("#") && tweetText.contains("@"));
                }
            }).flatMap(new FlatMapFunction<Text, Row>() {// Map the tweets to 3 columned data, each row showing username, hashtag, mention
                @Override
                public Iterable<Row> call(Text text) throws Exception {
                    ArrayList<Row> list = new ArrayList<Row>();
                    Tuple3<String, List<String>, List<String>> tweetInf = Util.parseTweet(text.toString());
                    for (String mentioned : tweetInf._2()) {
                        for (String hashtag : tweetInf._3()) {
                            list.add(RowFactory.create(tweetInf._1(), hashtag, mentioned));
                        }
                    }
                    return list;
                }
            });

            System.out.println("Number of tweets in filtered data: " + filteredTweets.count());

            //Build DataFrame from filteredTweet
            StructField[] tweetSchema = {
                    DataTypes.createStructField("username", DataTypes.StringType, true),
                    DataTypes.createStructField("hashtag", DataTypes.StringType, true),
                    DataTypes.createStructField("mention", DataTypes.StringType, true)
            };
            tweetData = sqlContext.createDataFrame(filteredTweets, new StructType(tweetSchema));
            //Show the schema and a few rows of data
            tweetData.printSchema();
            tweetData.show();

            //Save the filtered data as a parquet file
            tweetData.write().mode(SaveMode.Overwrite).parquet(outputPath + "filteredTweets");
        }

        //Load the filtered data from parquet file
        tweetData = sqlContext.read().parquet(outputPath + "filteredTweets");

        //Register the tweetData as temporary table, so we can run queries on it
        tweetData.registerTempTable("tweetTable");

        //Get users who have used #Youtube
        DataFrame youtubeUsers1 = tweetData.filter(tweetData.col("hashtag").$eq$eq$eq("Youtube")).select("username").distinct();
        System.out.println("(1) Number of distinct users using #Youtube: " + youtubeUsers1.count());
        youtubeUsers1.printSchema();
        youtubeUsers1.show();

        //Get the same results for users using #Youtube by running a query on the tweetData DataFrame
        DataFrame youtubeUsers = sqlContext.sql("SELECT distinct(username) FROM tweetTable WHERE hashtag LIKE 'Youtube'");
        System.out.println("(2) Number of distinct users using #Youtube: " + youtubeUsers1.count());
        youtubeUsers1.printSchema();
        youtubeUsers.show();


        DataFrame userMentionNumIranElections = sqlContext.sql("SELECT username, count(mention) AS iranElectionsMentionNumber FROM tweetTable WHERE hashtag LIKE 'IranElections' GROUP BY username");
        userMentionNumIranElections.printSchema();
        userMentionNumIranElections.show();
        System.out.println("\n(1) Number of distinct mentions by users using #Youtube: " + userMentionNumIranElections.count() + "\n");

        DataFrame userMentionNumIranians = sqlContext.sql("SELECT username, count(mention) AS iraniansMentionNumber FROM tweetTable WHERE hashtag LIKE 'iranians' GROUP BY username");
        System.out.println("\n(2) Number of distinct mentions by users using #Video: " + userMentionNumIranians.count() + "\n");
        userMentionNumIranians.printSchema();
        userMentionNumIranians.show();

        DataFrame joined = userMentionNumIranElections.join(userMentionNumIranians, userMentionNumIranElections.col("username").equalTo(userMentionNumIranians.col("username"))).drop(userMentionNumIranElections.col("username"));
        joined.printSchema();
        joined.show(100);
        System.out.println("\n(1) Number of results in joined1: " + joined.count() + "\n");


        //Do the same using sql query
        userMentionNumIranElections.registerTempTable("iranElectionsTable");
        userMentionNumIranians.registerTempTable("iraniansTable");
        DataFrame joined2 = sqlContext.sql("SELECT it.username, iet.iranElectionsMentionNumber, it.iraniansMentionNumber from iranElectionsTable iet INNER JOIN iraniansTable it ON iet.username = it.username");
        joined.printSchema();
        joined.show(100);
        System.out.println("\n(2) Number of results in joined2: " + joined2.count() + "\n");

        DataFrame userMentionNumH1N1= sqlContext.sql("SELECT mention, count(username) AS userCount FROM tweetTable WHERE (hashtag = 'H1N1' or hashtag = 'Oklahama') GROUP BY mention");
        System.out.println("\n(3) Number of distinct users using #H1N1 or #Oklahama: " + userMentionNumH1N1.count()+ "\n");
        userMentionNumH1N1.printSchema();
        userMentionNumH1N1.show();


        //Get a list of <mentions, users (as comma separated string)> who have used #H1N1
        StructField[] mentionUsersSchema = {
                DataTypes.createStructField("mention", DataTypes.StringType, true),
                DataTypes.createStructField("usernames", DataTypes.StringType, true)
        };
        DataFrame mentionH1N1Usernames = sqlContext.createDataFrame(tweetData.filter(tweetData.col("hashtag").$eq$eq$eq("H1N1")).distinct().drop("hashtag").javaRDD().mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                return new Tuple2<String, String>(row.getString(1), row.getString(0));
            }
        }).reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String v1, String v2) throws Exception {
                return v1 + "," + v2;
            }
        }).map(new Function<Tuple2<String, String>, Row>() {
            @Override
            public Row call(Tuple2<String, String> v1) throws Exception {
                return RowFactory.create(v1._1(), v1._2());
            }
        }), new StructType(mentionUsersSchema));

        //Write the DataFrame as parquet
        mentionH1N1Usernames.write().mode(SaveMode.Overwrite).parquet(outputPath + "hashtagH1N1MentionUsernames");

        //Write the DataFrame as CSV file
        mentionH1N1Usernames.coalesce(1).write().mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(outputPath + "hashtagH1N1MentionUsernames");
    }


}
