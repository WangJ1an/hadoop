package storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

import java.util.ArrayList;

/**
 * 组织各个处理组件形成一个完整的处理流程，就是所谓的 topology（类似于mapreduce中的job）
 * 并且将 topology 提交给storm集群去运行，topology提交到集群后就将一直运行，除非人为或异常退出
 * Created by 1 on 2017/6/17.
 */
public class TopoMain {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        TopologyBuilder builder = new TopologyBuilder();

        //将我们的spout组件设置到topology中去
        //parallelism_hint：4，表示4个excutor来执行这个组件
        //setNumTasks(8)，设置的是该组件执行时的并发task数量，也就意味着一个excutor执行2个task
        builder.setSpout("randomspout", new RandomWordSpout(), 4).setNumTasks(8);


        //将大写转换Bolt组件设置到topology，并且指定它接收randomspout组件的消息
        //.shuffleGrouping("randomspout")有两层含义：
        //1.upperbolt接收的一定是来自randomspout的Tuple
        //2.randomspout和upperbolt的大量并发task实例之间收发消息采用的分组策略是随机分组shuffleGrouping
        builder.setBolt("upperbolt", new UpperBolt(), 4).shuffleGrouping("randomspout");


        //将添加后缀的Bolt组件设置到topology，并且指定它接收upperbolt组件的消息
        builder.setBolt("suffixbolt", new SuffixBolt(),4).shuffleGrouping("upperbolt");


        //用builder来创建一个topology
        StormTopology topology = builder.createTopology();


        //配置一些topology在集群中运行时的参数
        Config conf = new Config();
        //这里设置的是整个demotopo所占用的槽位数，也就是worker的数量
        conf.setNumWorkers(4);
        conf.setDebug(true);
        conf.setNumAckers(0);


        //将这个topology提交给storm集群运行
        StormSubmitter.submitTopology("demotopo", conf, topology);

    }

}
