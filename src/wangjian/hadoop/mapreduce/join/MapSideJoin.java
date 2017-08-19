package wangjian.hadoop.mapreduce.join;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

/**
 * map端join 解决reduce端数据倾斜的问题
 * Created by 1 on 2017/7/5.
 */
public class MapSideJoin {

    static class MapSideJoinMapper extends Mapper<LongWritable,Text,Text,NullWritable> {
        private HashMap<String,String> hashMap = new HashMap<>();
        Text k = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //文件在该maptask的工作目录，即这段程序执行的当前目录

            BufferedReader  br = new BufferedReader(new InputStreamReader(new FileInputStream("product")));
          String line = null;
          while (StringUtils.isNotEmpty(line = br.readLine())) {
              String[] fields = line.split(",");
              hashMap.put(fields[0],fields[1]);
          }
          br.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String orderLine = value.toString();
            String[] orders = orderLine.split(",");
            String pname = hashMap.get(orders[2]);
            k.set(pname+"\t"+orders[0]+"\t"+orders[1]);
            context.write(k, NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(MapSideJoin.class);

        job.setMapperClass(MapSideJoinMapper.class);

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path("D:\\workspace\\hadoop\\src\\wangjian\\hadoop\\mapreduce\\join\\srcdata"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\workspace\\hadoop\\src\\wangjian\\hadoop\\mapreduce\\join\\outdata1"));

        // 指定需要缓存一个文件到所有的maptask运行节点工作目录
		/* job.addArchiveToClassPath(archive); */// 缓存jar包到task运行节点的classpath中
		/* job.addFileToClassPath(file); */// 缓存普通文件到task运行节点的classpath中
		/* job.addCacheArchive(uri); */// 缓存压缩包文件到task运行节点的工作目录
		/* job.addCacheFile(uri) */// 缓存普通文件到task运行节点的工作目录

        // 将产品表文件缓存到task工作节点的工作目录中去
        job.addCacheFile(new URI("file:/D:/product"));

        job.setNumReduceTasks(0);

        boolean res = job.waitForCompletion(true);
        System.exit(res ?0:1);
    }
}
