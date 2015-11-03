/*
 * BadRecordCount class 
 *
 * This is an MapReduce example of counting number of "bad tweets" in the Twitter data
 *
 * Author: Liping Liu
 * 
*/

package a4example;

import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public final class BadRecordCount {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: JavaWordCount <file>");
      System.exit(1);
    }

    boolean isEC2 = false;
    String path = args[0];
    // an example s3 buckent address: s3n://your_bucket/your_file

    SparkConf sparkConf = new SparkConf().setAppName("JavaBadRecordCount");
    Configuration hadoopConf = new Configuration();
    // blank line is used as delimeter
    // Note that the first record have have an extra line "total number:#" as its first line
    hadoopConf.set("textinputformat.record.delimiter", "\n\n");

    // If the submission is on EC2, you need to set access key and secret access key
    if (isEC2) 
    {
        hadoopConf.set("fs.s3n.awsAccessKeyId", "AKIAJCDSXTLDPIQNVGZA");
        hadoopConf.set("fs.s3n.awsSecretAccessKey", "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");
    }

    JavaSparkContext ctx = new JavaSparkContext(sparkConf);

    //Read the file with the function newAPIHadoopFile. The RDD object has elements like this: <lineNumber, textOfTweet>. 
    //With the function textFile in the Word Count example, the hadoopConf can not be passed in. 
    JavaPairRDD<LongWritable, Text> lines = ctx.newAPIHadoopFile(path, TextInputFormat.class, LongWritable.class, Text.class, hadoopConf);
         
    // A tweet has text "No Post Title" is considered as a bad record
    final String badMsg = "W\tNo Post Title";

    // In the class, MapReduce is introduced in a simple form. In Spark, map and reduce have more variants. Key-value pair <K, V> can be key 
    // <K> only.  This function maps a record  to <0> or <1>
    JavaRDD<Integer> flag = lines.map(new Function<scala.Tuple2<LongWritable, Text>, Integer>() {
      @Override
      public Integer call(scala.Tuple2<LongWritable, Text> tuple) {
        if (-1 == tuple._2().find(badMsg))
            return 0; 
        return 1;
      }
    });

    // In Spark, reduce function is applied on key-value pairs <K, V>, or keys <K>. 
    // The function reduceByKey actually corresponds to the reduce function in the class.
    Integer count = flag.reduce(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer a1, Integer a2) {
        return a1 + a2;
      }
    });

    //Print out the result
    System.out.println("Find " + count + " bad records in this file");

    ctx.stop();
  }
}


