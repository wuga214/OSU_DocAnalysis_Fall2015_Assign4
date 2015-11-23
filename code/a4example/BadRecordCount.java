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
import java.io.FileWriter;
import java.io.PrintWriter;

public final class BadRecordCount {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: JavaWordCount <file>");
      System.exit(1);
    }

    boolean isEC2 = true;
    String path = args[0];
    // an example s3 buckent address: s3n://your_bucket/your_file

    SparkConf sparkConf = new SparkConf().setAppName("JavaBadRecordCount");
    Configuration hadoopConf = new Configuration();
    // blank line is used as delimeter
    // Note that the first record have have an extra line "total number:#" as its first line
    hadoopConf.set("textinputformat.record.delimiter", "\n\n");

    // If the submission is on EC2, you need to set access key and secret access key
    String masterAdd = null;
    if (isEC2) 
    {
        hadoopConf.set("fs.s3n.awsAccessKeyId", "AKIaYOURaOWNaKEYaJPQ");
        hadoopConf.set("fs.s3n.awsSecretAccessKey", "v5vBmazYOURaOWNaSECRETaKEYaT8yX4jXC+mGLl");
        masterAdd = "ec2-54-213-21-124.us-west-2.compute.amazonaws.com";
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

    //Map lines to pairs. The key is the type of the record. 
    JavaPairRDD<String, Integer> typeFlag = lines.mapToPair(new PairFunction<Tuple2<LongWritable, Text>, String, Integer>() {
      @Override
      public Tuple2<String, Integer> call(Tuple2<LongWritable, Text> tuple) {
        if (-1 == tuple._2().find(badMsg))
            return new Tuple2<String, Integer>("bad", 1);
        return new Tuple2<String, Integer>("good", 1);
      }
    });

    //Reduce to get counts for each type 
    JavaPairRDD<String, Integer> typeCount = typeFlag.reduceByKey(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer a1, Integer a2) {
            return a1 + a2;
      }
    });

    try {

        //Save result to local file. First collect RDD to local list, and then save it. 
        PrintWriter wt = new PrintWriter(new FileWriter("result_local.txt"));
        List<Tuple2<String, Integer> > resultSet = typeCount.collect();

        for (Tuple2<String, Integer> elem : resultSet)
        {
            wt.println("(" + elem._1() + "," + elem._2() + ")");
        }
        wt.close();

        //Save result to hdfs file. The RDD object is directly saved to hdfs file 
        String fileName = "hdfs://" + masterAdd + ":9000/result_hdfs";
        typeCount.coalesce(1).saveAsTextFile(fileName);
    } catch (Exception e)
    {
        System.err.println("Cannot write result file.");
        System.err.println(e.getMessage());
    }


    ctx.stop();
  }
}


