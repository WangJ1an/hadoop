package kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;

import java.util.HashMap;

import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by 1 on 2017/6/20.
 */
public class ConsumerDemo {

    private static final String topic = "mysons";

    public static void main(String[] args) {

        Properties prop = new Properties();
        prop.put("zookeeper.connect", "weekend01:2181,weekend02:2181,weekend03:2181");
        prop.put("group.id","1111");
        prop.put("auto.offset.reset","smallest");

        ConsumerConfig config = new ConsumerConfig(prop);
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);

        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, 1);
        topicCountMap.put("mygirls", 1);
        topicCountMap.put("myboys", 1);
        Map<String, List<KafkaStream<byte[],byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[],byte[]>> streams = consumerMap.get("mygirls");

        for (final KafkaStream<byte[], byte[]> kafkaStream : streams) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for (MessageAndMetadata<byte[], byte[]> mm : kafkaStream) {
                        String msg = new String(mm.message());
                        System.out.println(msg);
                    }
                }
            }).start();
        }

    }


}
