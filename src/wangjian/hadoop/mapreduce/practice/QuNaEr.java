package wangjian.hadoop.mapreduce.practice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by 1 on 2017/6/21.
 */
public class QuNaEr {

    static class SearchMap extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split(" ");
            String info = strs[3];
            if (info.contains("dacheng")) {
                    context.write(new Text("dacheng"), new LongWritable(1));
            } else if (info.contains("wangfan")) {
                    context.write(new Text("wangfan"), new LongWritable(1));
            }
        }
    }

    static class SearchReduce extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for(LongWritable n : values) {
                count = (int) (count + n.get());
            }
            context.write(key, new LongWritable(count));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(QuNaEr.class);

        job.setMapperClass(SearchMap.class);
        job.setReducerClass(SearchReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job,new Path("D:\\workspace\\hadoop\\src\\wangjian\\hadoop\\mapreduce\\practice\\srcdata"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\workspace\\hadoop\\src\\wangjian\\hadoop\\mapreduce\\practice\\outdata"));

        job.waitForCompletion(true);
    }

}
