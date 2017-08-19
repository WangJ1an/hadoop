package wangjian.hadoop.mapreduce.groupComparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * Created by 1 on 2017/7/6.
 */
public class SecondarySort {
    static class SecondarySortMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

        OrderBean bean = new OrderBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] fields = StringUtils.split(line, ',');

            bean.set(new Text(fields[0]), new DoubleWritable(Double.parseDouble(fields[2])));

            context.write(bean, NullWritable.get());

        }

    }

    static class SecondarySortReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {


        //在设置了groupingcomparator以后，这里收到的kv数据 就是：  <1001 87.6>,null  <1001 76.5>,null  ....
        //此时，reduce方法中的参数key就是上述kv组中的第一个kv的key：<1001 87.6>
        //要输出同一个item的所有订单中最大金额的那一个，就只要输出这个key
        @Override
        protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            for (NullWritable nl :
                    values) {
                context.write(key, NullWritable.get());
                }
//             输出每个分组的全部key （即一个分组的全部数据）

            //只输出第一个key
//            context.write(key,NullWritable.get());
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(SecondarySort.class);

        job.setMapperClass(SecondarySortMapper.class);
        job.setReducerClass(SecondarySortReducer.class);


        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\workspace\\hadoop\\src\\wangjian\\hadoop\\mapreduce\\groupComparator\\srcdata"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\workspace\\hadoop\\src\\wangjian\\hadoop\\mapreduce\\groupComparator\\outdata1"));
        //指定shuffle所使用的GroupingComparator类
        job.setGroupingComparatorClass(ItemidGroupingComparator.class);
        //指定shuffle所使用的partitioner类
//        job.setPartitionerClass(ItemIdPartitioner.class);

//        job.setNumReduceTasks(3);

        job.waitForCompletion(true);

    }

}

