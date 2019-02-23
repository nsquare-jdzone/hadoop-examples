package com.javadeveloperzone;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

import static com.javadeveloperzone.MultipleOutputsDebugDriver.*;

public class MultipleOutputsMapper extends Mapper<Text,Text,Text,Text> {

    MultipleOutputs multipleOutputs;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs(context);
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        String empData[] = value.toString().split(",");

        if(empData[3].equalsIgnoreCase(AHMEDABAD)){
            multipleOutputs.write(AHMEDABAD,key,value, "AHMEDABAD/AHMEDABAD");
        }else if (empData[3].equalsIgnoreCase(DELHI)){
            multipleOutputs.write(DELHI,key,value,"DELHI/DELHI");
        }else if(empData[3].equalsIgnoreCase(MUMBAI)){
            multipleOutputs.write(MUMBAI,key,value,"MUMBAI/MUMBAI");
        }else {
            multipleOutputs.write(OTHER,key,value,"OTHER/OTHER");
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
