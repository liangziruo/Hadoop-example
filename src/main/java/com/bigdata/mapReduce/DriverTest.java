package com.bigdata.mapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DriverTest {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(new Configuration());
        //设置jar加载路径
        job.setJarByClass(DriverTest.class);
        //指定map
        job.setMapperClass(MapTest.class);
        //指定reduce
        job.setReducerClass(ReduceTest.class);

        //设置map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //设置最终输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置数据输入路径
        FileInputFormat.setInputPaths(job,new Path("F:\\test\\inputformat"));
        //设置数据输出路径
        FileOutputFormat.setOutputPath(job,new Path("F:\\test\\inputformat\\output4"));

        //一个文件会生成一个切片。从而会启动一个maptask。消耗过多资源
        //故：设置inputformat为CombineTextInputFormat。可以进一步设置最小切片值以及最大切片值
//        job.setInputFormatClass(CombineTextInputFormat.class);
//        CombineTextInputFormat.setMaxInputSplitSize(job,4194304);
//        CombineTextInputFormat.setMinInputSplitSize(job,2097152);

        //Combiner 用来对maptask节点进行小规模的提前合并，可以减小后去Reduce的IO传输，从而减小网络压力
        job.setCombinerClass(CombinerTest.class);

        //准备提交
        boolean wait = job.waitForCompletion(true);
        if (wait) {
            System.exit(wait ? 0 : 1);
        }
    }
}