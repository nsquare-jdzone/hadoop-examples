package com.javadeveloperzone;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        //For each key value pair, get the value and adds to the sum
        //to get the total occurrences of a word
        for(IntWritable value : values){
            sum = sum + value.get();
        }

        //Writes the word and total occurrences as key-value pair to the context
        context.write(key, new IntWritable(sum));
    }
}
