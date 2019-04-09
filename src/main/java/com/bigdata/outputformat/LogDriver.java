package com.bigdata.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration cf = new Configuration();
        Job job = Job.getInstance(cf);

        job.setJarByClass(LogDriver.class);
        job.setMapperClass(LogMap.class);
        job.setReducerClass(LogReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(LogOutPutFormat.class);

        FileInputFormat.setInputPaths(job,new Path("E:\\Software\\test\\exp.txt"));

        FileOutputFormat.setOutputPath(job,new Path("E:\\Software\\test\\outputlog"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
