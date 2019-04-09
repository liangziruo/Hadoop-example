package com.bigdata.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WholeRecordReader extends RecordReader<NullWritable,BytesWritable>{

    private FileSplit fs;
    private BytesWritable value = new BytesWritable();
    private Configuration configuration;
    private boolean flag = false;
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        fs = (FileSplit)split;
         configuration = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if(!flag) {
            byte[] bytes = new byte[(int) fs.getLength()];
            Path path = fs.getPath();
            FileSystem fileSystem = path.getFileSystem(configuration);
            FSDataInputStream fis = fileSystem.open(path);

            IOUtils.readFully(fis, bytes, 0, bytes.length);

            value.set(bytes, 0, bytes.length);

            IOUtils.closeStream(fis);
            flag = true;
            return true;
        }
        flag = false;
        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return flag?1:0;
    }

    @Override
    public void close() throws IOException {

    }
}
