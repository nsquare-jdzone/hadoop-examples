package com.javadeveloperzone;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        /*
         * 1. convert to lower case
         * 2. replace all punctuation mark characters with SPACE
         * 3. Tokenize input line
         * 4. write it to HDFS
         *  */
        String line = value.toString().toLowerCase().replaceAll("\\p{Punct}"," ");
        StringTokenizer st = new StringTokenizer(line," ");
        //For each token, write a key value pair with
        //word and 1 as value to context
        while(st.hasMoreTokens()){
            word.set(st.nextToken());
            context.write(word,one);
        }
    }
}
