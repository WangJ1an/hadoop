package kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * Created by 1 on 2017/6/19.
 */
public class ProducerDemo {

    public static void main(String[] args) throws InterruptedException {
        Properties prop = new Properties();
        prop.put("zk.connect", "weekend01:2181,weekend02:2181,weekend03:2181");
        prop.put("metadata.broker.list", "weekend01:9092,weekend02:9092,weekend03:9092");
        prop.put("serializer.class", "kafka.serializer.StringEncoder");

        ProducerConfig config = new ProducerConfig(prop);
        Producer<String,String> producer = new Producer<String, String>(config);
        //发送业务消息
        //读取文件 读取内存数据库 读Socket端口
        for(int i = 1; i <= 1000; i++) {
            Thread.sleep(500);
            producer.send(new KeyedMessage<String,String>("myboys","i said i love you baby for" + i + "times"));
        }
    }
}
