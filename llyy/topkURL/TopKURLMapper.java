package llyy.topkURL;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import wangjian.hadoop.mapreduce.flowsum.FlowBean;

import java.io.IOException;

/**
 * Created by 1 on 2017/6/22.
 */
public class TopKURLMapper extends Mapper<LongWritable,Text,Text,FlowBean> {

    private FlowBean bean = new FlowBean();
    private Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] fields = StringUtils.split(line,'\t');

        try {
            if (fields.length > 27 && !fields[15].isEmpty()
                    && fields[15].startsWith("http")) {
                String url = fields[15];

                long up_flow = Long.parseLong(fields[23]);
                long down_flow = Long.parseLong(fields[24]);

                k.set(url);
                bean.set("", up_flow, down_flow);

                context.write(k, bean);
            }
        }catch (Exception e) {

        }
    }
}
