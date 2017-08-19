package llyy.topkURL;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import wangjian.hadoop.mapreduce.flowsum.FlowBean;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by 1 on 2017/6/22.
 */
public class TopKURLReducer extends Reducer<Text,FlowBean,Text,LongWritable> {

    private TreeMap<FlowBean,String> treeMap = new TreeMap<>();
    private double globalCount;
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context)
            throws IOException, InterruptedException {

        long up_sum = 0;
        long down_sum = 0;

        for (FlowBean bean :
                values) {
            up_sum += bean.getUp_flow();
            down_sum += bean.getDown_flow();
        }

        FlowBean bean = new FlowBean("",up_sum,down_sum);

        globalCount += bean.getS_flow();

        treeMap.put(bean,key.toString());

    }

    //reduce方法完成后即将退出reduce任务时会调用该方法
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        double tempCount = 0;
        for (Map.Entry<FlowBean, String> entry : treeMap.entrySet()) {
            if (tempCount / globalCount < 0.8) {
                context.write(new Text(entry.getValue()), new LongWritable(entry.getKey().getS_flow()));
                tempCount = entry.getKey().getS_flow();
            } else {
                return;
            }
        }

    }
}
