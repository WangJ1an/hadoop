package practice.findfriend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by 1 on 2017/7/5.
 */
public class FindFriendRunner extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FindFriendRunner.class);
        job.setMapperClass(FindFriendMapper.class);
        job.setReducerClass(FindFriendReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path("D:\\workspace\\hadoop\\src\\practice\\findfriend\\srcdata"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\workspace\\hadoop\\src\\practice\\findfriend\\outdata"));

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new FindFriendRunner(),args);
        System.exit(res);
    }
}
