package wangjian.hadoop.mapreduce.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by 1 on 2017/7/4.
 */
public class ReduceSideJoin {

    static class RJoinMapper extends Mapper<LongWritable,Text,Text,Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //获取文件名
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            String name = inputSplit.getPath().getName();
            //通过文件名判断是哪种数据
            if (name.startsWith("order")) {
                String[] fields = value.toString().split(",");
                String str = "<order>,"+fields[0]+","+fields[1]+","+fields[3];
                context.write(new Text(fields[2]),new Text(str));
            } else if (name.startsWith("product")) {
                String[] fields = value.toString().split(",");
                String str = "<product>,"+fields[1]+","+fields[2]+","+fields[3];
                context.write(new Text(fields[0]),new Text(str));
            }
        }
    }

    static class RJoinReduce extends Reducer<Text,Text,Text,NullWritable> {
        private String order_id;
        private String order_date;
        private String product_name;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> arrayList = new ArrayList<>();
            for (Text text : values) {
                String[] fields = text.toString().split(",");
                if (fields[0].equals("<order>")) {
                    order_id = fields[1];
                    order_date = fields[2];
                    arrayList.add(order_id+"    "+order_date);
                } else if (fields[0].equals("<product>")) {
                    product_name = fields[1];
                }
            }

            for (int i = 0; i < arrayList.size(); i++) {
                context.write(new Text(arrayList.get(i) + "   " + product_name), NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(ReduceSideJoin.class);
        job.setMapperClass(RJoinMapper.class);
        job.setReducerClass(RJoinReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path("D:\\workspace\\hadoop\\src\\wangjian\\hadoop\\mapreduce\\join\\srcdata"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\workspace\\hadoop\\src\\wangjian\\hadoop\\mapreduce\\join\\outdata"));

        job.waitForCompletion(true);
    }
}
