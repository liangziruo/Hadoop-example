package com.bigdata.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class ZdyRecordWriter extends RecordWriter<Text, NullWritable> {
    FileSystem fileSystem = null;
    FSDataOutputStream baidu = null;
    FSDataOutputStream other = null;
    public ZdyRecordWriter(TaskAttemptContext job) throws IOException {
        Configuration configuration = job.getConfiguration();
        fileSystem = FileSystem.get(configuration);
        baidu = fileSystem.create(new Path("E:\\Software\\test\\outputlog\\baidu.log"));
         other = fileSystem.create(new Path("E:\\Software\\test\\outputlog\\other.log"));

    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {

        if (key.toString().contains("baidu")){
            baidu.write(key.toString().getBytes());
        }else{
            other.write(key.toString().getBytes());
        }

    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {

        baidu.close();
        other.close();
        fileSystem.close();
    }
}
