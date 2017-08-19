package wangjian.hadoop.mapreduce.groupComparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by 1 on 2017/7/6.
 */
public class ItemIdPartitioner extends Partitioner<OrderBean,NullWritable> {
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numPartitions) {
        return (orderBean.getItemid().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
