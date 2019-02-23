package com.javadeveloperzone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MultipleOutputsDriver extends Configured implements Tool {


    public static final String OTHER = "OTHER";
    public static final String MUMBAI = "MUMBAI";
    public static final String DELHI = "DELHI";
    public static final String AHMEDABAD = "AHMEDABAD";

    public static void main(String[] args) throws Exception {

        int exitCode = ToolRunner.run(new Configuration(),
                new MultipleOutputsDriver(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.println("Please provid two arguments :");
            System.out.println("[ 1 ] Input dir path");
            System.out.println("[ 2 ] Output dir path");
            return -1;
        }

        Configuration c=new Configuration();
        String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
        Path input=new Path(files[0]);
        Path output=new Path(files[1]);
        Job job=Job.getInstance();
        job.setJarByClass(MultipleOutputsDriver.class);
        job.setMapperClass(MultipleOutputsMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        MultipleOutputs.addNamedOutput(job,"AHMEDABAD", TextOutputFormat.class,Text.class,Text.class);
        MultipleOutputs.addNamedOutput(job,"DELHI", TextOutputFormat.class,Text.class,Text.class);
        MultipleOutputs.addNamedOutput(job,"MUMBAI", TextOutputFormat.class,Text.class,Text.class);
        MultipleOutputs.addNamedOutput(job,"OTHER", TextOutputFormat.class,Text.class,Text.class);
        boolean success = job.waitForCompletion(true);
        return (success?0:1);
    }
}
