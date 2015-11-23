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

    is_ec2 = True 
    # If the submission is on EC2, you need to set access key and secret access key
    if is_ec2:
        hadoop_conf["fs.s3n.awsAccessKeyId"] = "AKIaYOURaOWNaKEYaJPQ"
        hadoop_conf["fs.s3n.awsSecretAccessKey"] = "v5vBmazYOURaOWNaSECRETaKEYaT8yX4jXC+mGLl"
        master_add = "ec2-54-213-21-124.us-west-2.compute.amazonaws.com"

    # Read the file with the function newAPIHadoopFile. The RDD object has elements like this: <lineNumber, textOfTweet>. 
    # With the function textFile in the Word Count example, the hadoopConf can not be passed in. 
    lines = sc.newAPIHadoopFile(filepath, "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                                "org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text",
                                conf=hadoop_conf)

    # print out records to understand the logic of RDD.
    # NOTE: you can not print an RDD object directly. Take a small sample from it 
    #print(lines.take(5))

    # A tweet has text "No Post Title" is considered as a bad record
    bad_msg = "W\tNo Post Title"

    # In the class, MapReduce is introduced in a simple form. In Spark, map and reduce have more variants. Key-value pair <K, V> can be key 
    # <K> only.  This function maps a record  to <0> or <1>
    flag = lines.map(lambda x: 0 if -1 == string.find(x[1], bad_msg) else 1) 

    # print mapped keys
    #print(flag.take(5)) 

    # In Spark, reduce function is applied on key-value pairs <K, V>, or keys <K>. 
    # The function reduceByKey actually corresponds to the reduce function in the class.
    count = flag.reduce(add)


    #Print out the result
    print("Find %d bad records in this file" %count)

    #Map lines to pairs. The key is the type of the record. 
    type_flag = lines.map(lambda x: ("good", 1) if -1 == string.find(x[1], bad_msg) else ("bad", 1))

    #Reduce to get counts for each type 
    type_count = type_flag.reduceByKey(add);

    #Save result to local file. First collect RDD to local list, and then save it. 
    result_set = type_count.collect() 
    local_file = open("result_local.txt", "w")
    for e in result_set:
        local_file.write('(' + str(e[0])  + ',' + str(e[1]) + ')\n') 
    local_file.close()

    #Save result to hdfs file. The RDD object is directly saved to hdfs file 
    fileName = "hdfs://" + master_add + ":9000/result_hdfs";
    type_count.coalesce(1).saveAsTextFile(fileName)

    sc.stop()
