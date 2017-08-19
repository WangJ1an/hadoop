package practice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import wangjian.hadoop.mapreduce.flowsum.FlowBean;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by 1 on 2017/7/1.
 */
public class FlowRunner extends Configured implements Tool{
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance();

        job.setJarByClass(FlowRunner.class);

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setOutputKeyClass(FlowBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setPartitionerClass(ProvincePartitioner.class);
//        job.setNumReduceTasks(5); // 应与partitioner 分区数相同

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(job,new Path("D:\\workspace\\hadoop\\src\\practice\\flowdata"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\workspace\\hadoop\\src\\practice\\outdata"));

        return job.waitForCompletion(true)?0:1;
    }

    static class FlowMapper extends Mapper<LongWritable,Text,Text,FlowBean> {
        private FlowBean flowBean = new FlowBean();
        private Text text = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = StringUtils.split(line,'\t');

            String phoneNb = fields[1];
            int up_flow = Integer.parseInt(fields[8]);
            int down_flow = Integer.parseInt(fields[9]);
            flowBean.set(phoneNb, up_flow, down_flow);
            text.set(phoneNb);
            context.write(text,flowBean);

        }
    }

    static class FlowReducer extends Reducer<Text,FlowBean,FlowBean,NullWritable> {

        private TreeMap<FlowBean, NullWritable> treeMap;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            treeMap = new TreeMap<>();
        }

        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

            int up_flow_sum = 0;
            int down_flow_sum = 0;

            for (FlowBean b :
                    values) {
                up_flow_sum += b.getUp_flow();
                down_flow_sum += b.getDown_flow();
            }

            treeMap.put(new FlowBean(key.toString(),up_flow_sum,down_flow_sum),NullWritable.get());
        }


        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<FlowBean, NullWritable> entry : treeMap.entrySet()){
                FlowBean bean = entry.getKey();
                context.write(bean,NullWritable.get());
            }
        }
    }

    static class ProvincePartitioner extends Partitioner<Text ,FlowBean> {

        private static final HashMap<String, Integer> hashMap = new HashMap<>();
        static {
            hashMap.put("135",0);
            hashMap.put("136",1);
            hashMap.put("134",2);
            hashMap.put("139",3);
        }

        @Override
        public int getPartition(Text text, FlowBean flowBean, int i) {
            Integer areaNum = hashMap.get(text.toString().substring(0,3));
            areaNum = areaNum == null ? 4 : areaNum;
            return areaNum;
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),new FlowRunner(),args);
        System.exit(res);
    }
}
