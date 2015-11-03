#
# bad_record_count.py
#
# This example shows how to count number of bad records in the tweet data with MapReduce 
# Any record with message content "No Post Title" is considered as a bad record. 
# #

from __future__ import print_function

import sys
import string 
from operator import add

from pyspark import SparkContext


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: bad_record_count <file>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonBadRecordCount")

    filepath = sys.argv[1]
    # an example of s3 file path: s3n://your_bucket/your_file

    # blank line is used as delimeter
    # Note that the first record have have an extra line "total number:#" as its first line
    hadoop_conf = {"textinputformat.record.delimiter": "\n\n"} 

    is_ec2 = False
    # If the submission is on EC2, you need to set access key and secret access key
    if is_ec2:
        hadoop_conf["fs.s3n.awsAccessKeyId"] = "AKIAJCDSXTLDPIQNVGZA"
        hadoop_conf["fs.s3n.awsSecretAccessKey"] = "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"

    # Read the file with the function newAPIHadoopFile. The RDD object has elements like this: <lineNumber, textOfTweet>. 
    # With the function textFile in the Word Count example, the hadoopConf can not be passed in. 
    records = sc.newAPIHadoopFile(filepath, "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                                "org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text",
                                conf=hadoop_conf)

    # print out records to understand the logic of RDD.
    # NOTE: you can not print an RDD object directly. Take a small sample from it 
    #print(records.take(5))

    # A tweet has text "No Post Title" is considered as a bad record
    bad_msg = "W\tNo Post Title"

    # In the class, MapReduce is introduced in a simple form. In Spark, map and reduce have more variants. Key-value pair <K, V> can be key 
    # <K> only.  This function maps a record  to <0> or <1>
    flag = records.map(lambda x: 0 if -1 == string.find(x[1], bad_msg) else 1) 

    # print mapped keys
    #print(flag.take(5)) 

    # In Spark, reduce function is applied on key-value pairs <K, V>, or keys <K>. 
    # The function reduceByKey actually corresponds to the reduce function in the class.
    count = flag.reduce(add)


    #Print out the result
    print("Find %d bad records in this file" %count)

    sc.stop()
