package wangjian.hadoop.mapreduce.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * FlowBean是我们自定义的一种数据类型，要在hadoop各个节点之间传输，应该遵循hadoop的序列化机制
 *
 * Created by 1 on 2017/5/17.
 */
public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        String[] fields = StringUtils.split(line, '\t');

        String phoneNB = fields[0];
        long up_flow = Long.parseLong(fields[7]);
        long down_flow = Long.parseLong(fields[8]);

        context.write(new Text(phoneNB),new FlowBean(phoneNB,up_flow,down_flow));
    }
}
