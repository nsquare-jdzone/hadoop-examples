package com.javadeveloperzone;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PassengerRushReducer extends Reducer<GeoLocationWritable, IntWritable, Text, IntWritable> {

    IntWritable totalPassenger;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        totalPassenger = new IntWritable(0);
    }

    @Override
    protected void reduce(GeoLocationWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        for(IntWritable passender : values){
            sum+=passender.get();
        }
        totalPassenger.set(sum);
        context.write(new Text(key.getLocationName()),totalPassenger);
    }
}
