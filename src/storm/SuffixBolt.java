package storm;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.io.FileWriter;
import java.util.Map;
import java.util.UUID;

/**
 * Created by 1 on 2017/6/17.
 */
public class SuffixBolt extends BaseBasicBolt {

    private FileWriter fileWriter;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        try {
            fileWriter = new FileWriter("/home/hadoop/stormoutput/"+ UUID.randomUUID());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
    }

    //该bolt的核心处理逻辑
    //每收到一个tuple消息就会被调用一次
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String upper_name = tuple.getString(0);
        String suffix_name = upper_name + "_it's ok";

        try {
            fileWriter.write(suffix_name+"\n");
            fileWriter.flush();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    //该bolt已不需要发送数据到下一组件，所以不需要再声明tuple的字段
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
