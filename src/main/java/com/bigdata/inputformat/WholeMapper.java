package com.bigdata.inputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WholeMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable>{

    Text k = new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        FileSplit split = (FileSplit)context.getInputSplit();

        String pathName = split.getPath().toString();

        k.set(pathName);
    }

    @Override
    protected void map(NullWritable key, BytesWritable value,
                       Context context)
            throws IOException, InterruptedException {

        context.write(k, value);
    }

}
