package com.javadeveloperzone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;


public class WordCountOutputFormat<K,V> extends FileOutputFormat<K, V> {
    public static String FIELD_SEPARATOR = "mapreduce.output.textoutputformat.separator";
    public static String RECORD_SEPARATOR = "mapreduce.output.textoutputformat.recordseparator";

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();
        boolean isCompressed = getCompressOutput(job);
        String fieldSeprator = conf.get(FIELD_SEPARATOR, "\t");
        //custom record separator, \n used as a default
        String recordSeprator = conf.get(RECORD_SEPARATOR, "\n");

        //compress output logic
        CompressionCodec codec = null;
        String extension = "";
        if (isCompressed) {
            Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
            codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
            extension = codec.getDefaultExtension();
        }

        Path file = this.getDefaultWorkFile(job, extension);
        FileSystem fs = file.getFileSystem(conf);
        FSDataOutputStream fileOut = fs.create(file, false);
        if(isCompressed){
            return new WordCountLineRecordWriter<>(new DataOutputStream(codec.createOutputStream(fileOut)), fieldSeprator,recordSeprator);
        }else{
            return new WordCountLineRecordWriter<>(fileOut, fieldSeprator,recordSeprator);
        }
    }


}
