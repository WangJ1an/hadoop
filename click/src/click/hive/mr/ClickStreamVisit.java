package click.hive.mr;

import click.hive.mr.mrbean.PageViewsBean;
import click.hive.mr.mrbean.VisitBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by 1 on 2017/7/19.
 */
public class ClickStreamVisit extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(ClickStreamVisit.class);
        job.setMapperClass(ClickStreamVisitMapper.class);
        job.setReducerClass(ClickStreamVisitReducer.class);

        job.setMapOutputKeyClass(PageViewsBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(VisitBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setGroupingComparatorClass(CSVGroupCompartor.class);
        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job,new Path("D:\\workspace\\hadoop\\click\\src\\pageviews"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\workspace\\hadoop\\click\\src\\visits"));

        return job.waitForCompletion(true)?0:1;
    }

    static class ClickStreamVisitMapper extends Mapper<LongWritable,Text,PageViewsBean,NullWritable> {
        PageViewsBean pvBean = new PageViewsBean();
        NullWritable nullWritable = NullWritable.get();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\\001");
            int step = Integer.parseInt(fields[5]);
            //(String session, String remote_addr, String timestr, String request, int step, String staylong, String referal, String useragent, String bytes_send, String status)
            //299d6b78-9571-4fa9-bcc2-f2567c46df3472.46.128.140-2013-09-18 07:58:50/hadoop-zookeeper-intro/160"https://www.google.com/""Mozilla/5.0"14722200
            pvBean.set(fields[0], fields[1], fields[2], fields[3],fields[4], step, fields[6], fields[7], fields[8], fields[9]);
            context.write(pvBean,nullWritable);
        }
    }

    static class ClickStreamVisitReducer extends Reducer<PageViewsBean,NullWritable,VisitBean,NullWritable> {
        VisitBean visitBean = new VisitBean();
        NullWritable nullWritable = NullWritable.get();

        @Override
        protected void reduce(PageViewsBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            ArrayList<PageViewsBean> pvBeansList = new ArrayList<>();
            for (NullWritable n :
                    values) {
                pvBeansList.add(key);
            }
            // 取visit的首记录
            visitBean.setInPage(pvBeansList.get(0).getRequest());
            visitBean.setInTime(pvBeansList.get(0).getTimestr());
            // 取visit的尾记录
            visitBean.setOutPage(pvBeansList.get(pvBeansList.size() - 1).getRequest());
            visitBean.setOutTime(pvBeansList.get(pvBeansList.size() - 1).getTimestr());
            // visit访问的页面数
            visitBean.setPageVisits(pvBeansList.size());
            // 来访者的ip
            visitBean.setRemote_addr(pvBeansList.get(0).getRemote_addr());
            // 本次visit的referal
            visitBean.setReferal(pvBeansList.get(0).getReferal());
            visitBean.setSession(key.getSession());

            context.write(visitBean,nullWritable);
        }
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new ClickStreamVisit(),args);
    }
}
