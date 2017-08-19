package click.hive.mr.pre;

import click.hive.mr.mrbean.WebLogBean;
import click.hive.mr.mrbean.WebLogParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by 1 on 2017/7/16.
 */
public class WebLogPreProcess {
    static class ProcessMapper extends Mapper<LongWritable,Text,Text,NullWritable> {
        Set<String> pages = new HashSet<>();
        Text k = new Text();
        NullWritable v = NullWritable.get();


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            pages.add("/about");
            pages.add("/black-ip-list/");
            pages.add("/cassandra-clustor/");
            pages.add("/finance-rhive-repurchase/");
            pages.add("/hadoop-family-roadmap/");
            pages.add("/hadoop-hive-intro/");
            pages.add("/hadoop-zookeeper-intro/");
            pages.add("/hadoop-mahout-roadmap/");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            WebLogBean webLogBean = WebLogParser.parser(line);
            //过滤js，图片，css等静态资源
            WebLogParser.filtStaticResource(webLogBean,pages);
            k.set(webLogBean.toString());
            context.write(k,v);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(WebLogPreProcess.class);
        job.setMapperClass(ProcessMapper.class);

        job.setMapOutputKeyClass(WebLogBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path("D:\\workspace\\hadoop\\click\\src\\srcdata"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\workspace\\hadoop\\click\\src\\outdata"));

        job.setNumReduceTasks(0);
        job.waitForCompletion(true);
    }
}
