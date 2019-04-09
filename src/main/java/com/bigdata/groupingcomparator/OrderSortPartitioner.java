package com.bigdata.groupingcomparator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author admin
 */
public class OrderSortPartitioner extends Partitioner<OrderBean, NullWritable>{

    @Override
    public int getPartition(OrderBean key, NullWritable value, int numReduceTasks) {

        return (key.getOrder_id() & Integer.MAX_VALUE) % numReduceTasks;
    }

}
