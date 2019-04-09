package com.bigdata.mapReduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CombinerTest extends Reducer<Text,LongWritable,Text,LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (LongWritable lw : values){
            count += lw.get();
        }
        context.write(key,new LongWritable(count));
    }
}
