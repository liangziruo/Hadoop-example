package com.bigdata.helloworld;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DriverHello {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //指定jar所在路径
        job.setJarByClass(DriverHello.class);
        //指定mapper类
        job.setMapperClass(HelloMap.class);
        //指定Reduce类
        job.setReducerClass(ReduceHello.class);

        //指定map输出k和v的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //指定最终输出k和v类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //指定数据源路径
        FileInputFormat.setInputPaths(job,new Path("E:\\111.txt"));

        //指定数据输出路径
        FileOutputFormat.setOutputPath(job,new Path("E:\\output"));


//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        //提交
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);

    }
}
