package storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Created by 1 on 2017/6/17.
 */
public class UpperBolt extends BaseBasicBolt {

    //业务处理逻辑
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        //获取上一个组件传递过来的数据，数据在tuple里
        String godName = tuple.getString(0);

        String godName_upper = godName.toUpperCase();

        basicOutputCollector.emit(new Values(godName_upper));
    }

    //声明该bolt组件要发送出去的tuple的字段
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields("upperName"));

    }
}
