package wangjian.hadoop.mapreduce.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by 1 on 2017/5/17.
 */
public class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context)
            throws IOException, InterruptedException {

        long up_flow_counter = 0;
        long down_flow_counter = 0;

        for (FlowBean bean : values) {

            up_flow_counter += bean.getUp_flow();
            down_flow_counter += bean.getDown_flow();

        }

        context.write(key,new FlowBean(key.toString(),up_flow_counter,down_flow_counter));
    }
}
