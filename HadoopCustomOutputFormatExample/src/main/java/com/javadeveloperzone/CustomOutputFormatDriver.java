package com.javadeveloperzone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CustomOutputFormatDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {

        int exitCode = ToolRunner.run(new Configuration(),
                new CustomOutputFormatDriver(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.println("Please provide two arguments :");
            System.out.println("[ 1 ] Input dir path");
            System.out.println("[ 2 ] Output dir path");
            return -1;
        }

        Configuration c=new Configuration();
        String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
        Path input=new Path(files[0]);
        Path output=new Path(files[1]);
        Configuration conf=new Configuration();

        /*
        * UnComment below three lines to enable local debugging of map reduce job
         * */
        /*conf.set("fs.defaultFS", "local");
        conf.set("mapreduce.job.maps","1");
        conf.set("mapreduce.job.reduces","1");
        */

        Job job=Job.getInstance(conf,"Hadoop Custom Output Format Example");
        job.setJarByClass(CustomOutputFormatDriver.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(WordCountOutputFormat.class);
        job.setNumReduceTasks(1);
        //Custom record separator, set in job configuration
        job.getConfiguration().set("mapreduce.output.textoutputformat.recordseparator","<==>");
        //set field separator other than default \t character
        job.getConfiguration().set("mapreduce.output.textoutputformat.separator",";");
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        job.setSpeculativeExecution(false);
        boolean success = job.waitForCompletion(true);
        return (success?0:1);
    }
}
