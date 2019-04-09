package com.bigdata.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class WholeDriver  {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration cfg = new Configuration();
        Job job = Job.getInstance(cfg);

        job.setJarByClass(WholeDriver.class);

        job.setMapperClass(WholeMapper.class);
        job.setReducerClass(WholeReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);


        //指定读取文件的输入流为自定义输入流WholeFileInputFormat
        job.setInputFormatClass(WholeFileInputFormat.class);
        //指定输出流为SequenceFileOutputFormat
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(job,new Path("F:\\test\\inputformat\\"));
        FileOutputFormat.setOutputPath(job,new Path("F:\\test\\inputformat\\output"));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);

    }
}
