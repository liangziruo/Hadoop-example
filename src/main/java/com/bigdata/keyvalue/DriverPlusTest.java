package com.bigdata.keyvalue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DriverPlusTest {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {

        //获取Configuration
        Configuration configuration = new Configuration();

        configuration.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR," ");
        //获取Job
        Job job = Job.getInstance(configuration);

        //设置jar
        job.setJarByClass(DriverPlusTest.class);

        //设置对应的map和reduce类
        job.setMapperClass(MapPlusTest.class);
        job.setReducerClass(ReducePlusTest.class);

        //设置map的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置最后的map输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置输入和输出的路径
        FileInputFormat.setInputPaths(job,new Path("E:\\aaa"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\aaa\\output1"));

        job.setInputFormatClass(KeyValueTextInputFormat.class);
//        job.setInputFormatClass(CombineTextInputFormat.class);
//        CombineTextInputFormat.setMaxInputSplitSize(job,4194304);
//        CombineTextInputFormat.setMinInputSplitSize(job,2097152);
        boolean b = job.waitForCompletion(true);

        System.exit(b?0:1);

    }
}
