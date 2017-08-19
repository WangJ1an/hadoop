package wangjian.hadoop.mapreduce.areapartition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import wangjian.hadoop.mapreduce.flowsum.FlowBean;

import java.io.IOException;

/**
 * 对流量原始日志进行统计分析，将不同省份的用户统计结果输出到不同的文件
 * 需要自定义改造两个机制：
 * 1.改造分区的逻辑，自定义一个partitioner
 * 2.自定义reduce task的并发任务数
 *
 * Created by 1 on 2017/5/18.
 */
public class FlowSumArea {

   public static class FlowSumAreaMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

       @Override
       protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

           String line = value.toString();

           String[] fields = StringUtils.split(line, '\t');

           String phoneNB = fields[0];

           long u_flow = Long.parseLong(fields[1].trim());

           long d_flow = Long.parseLong(fields[2].trim());

           context.write(new Text(phoneNB), new FlowBean(phoneNB, u_flow, d_flow));
       }

   }


   public static class FlowSumAreaReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

       @Override
       protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

           long u_flow_counter = 0;
           long d_flow_counter = 0;

           for (FlowBean fb : values) {

               u_flow_counter += fb.getUp_flow();
               d_flow_counter += fb.getDown_flow();

           }

           context.write(key, new FlowBean(key.toString(), u_flow_counter, d_flow_counter));

       }
   }

   public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

       Configuration conf = new Configuration();
       Job job = Job.getInstance();

       job.setJarByClass(FlowSumArea.class);

       job.setMapperClass(FlowSumAreaMapper.class);
       job.setReducerClass(FlowSumAreaReducer.class);

       //设置我们自定义的分组逻辑定义
       job.setPartitionerClass(AreaPartitioner.class);

       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(FlowBean.class);

       //设置reduce的任务并发数，应该和分组数保持一致
       job.setNumReduceTasks(6);

       FileInputFormat.setInputPaths(job, new Path(args[0]));
       FileOutputFormat.setOutputPath(job, new Path(args[1]));

       System.exit(job.waitForCompletion(true) ? 0 : 1);
   }

}
