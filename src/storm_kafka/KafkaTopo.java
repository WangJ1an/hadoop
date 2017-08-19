package storm_kafka;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

/**
 * Created by 1 on 2017/6/20.
 */
public class KafkaTopo {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

        String topic = "wordcount";
        String zkRoot = "/kafka-storm";
        String spoutId = "kafkaSpout";
        BrokerHosts brokerHosts =
                new ZkHosts("weekend01:2181,weekend02:2181,weekend03:2181");
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts,topic,zkRoot,spoutId);
        spoutConfig.forceFromStart = true;
        spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(spoutId,new KafkaSpout(spoutConfig));
        builder.setBolt("word-spliter",new WordSpliter()).shuffleGrouping(spoutId);
        builder.setBolt("word-writer",new WriterBolt()).fieldsGrouping("word-spliter",new Fields("word"));

        Config config = new Config();
        config.setNumWorkers(4);
        config.setNumAckers(0);
        config.setDebug(false);

        //LocalCluster用来将topology提交到本地模拟器运行，方便开发调试
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("WordCount", config, builder.createTopology());

        //提交topology到storm集群中运行
		StormSubmitter.submitTopology("Word-Count", config, builder.createTopology());
    }

}
