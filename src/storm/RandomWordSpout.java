package storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * Created by 1 on 2017/6/17.
 */
public class RandomWordSpout extends BaseRichSpout{

    //初始化方法，在Spout组件实例化时调用一次
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    private SpoutOutputCollector collector;

    //模拟一些数据
    private String[] words = {"iphone","xiaomi","meizu","suoni","sanxing"};

    //不断往下一个组件发送tuple消息
    //怎样发送下一个Tuple，这里是该spout组件的核心逻辑
    @Override
    public void nextTuple() {
        //可以从kafka消息队列中拿到数据，简便起见，我们从words数组中随机挑选一个商品名发送
        Random random = new Random();
        int index = random.nextInt(words.length);

        String godName = words[index];

        //将商品名封装成Tuple，发送消息给下一个组件
        collector.emit(new Values(godName));

        //每发送一个消息休眠500ms
        Utils.sleep(500);


    }

    //声明输出的Tuple里字段的定义
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields("orignName"));

    }
}
