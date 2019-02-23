package com.javadeveloperzone;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PassengerRushMapper extends Mapper<Text,Text,GeoLocationWritable, IntWritable> {

    IntWritable intKey;
    GeoLocationWritable geoLocationWritable;

    @Override
    protected void setup(Context context){
        intKey = new IntWritable(0);
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        String taxiData[] = value.toString().split(",");
        intKey.set(Integer.parseInt(taxiData[4]));
        geoLocationWritable = new GeoLocationWritable();
        geoLocationWritable.setLatitude(new DoubleWritable(Double.parseDouble(taxiData[0])));
        geoLocationWritable.setLongitude(new DoubleWritable(Double.parseDouble(taxiData[1])));
        geoLocationWritable.setLocationName(new Text(taxiData[2]));

        context.write(geoLocationWritable,intKey );
    }

}
