package storm_kafka;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.apache.velocity.runtime.Runtime;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * Created by 1 on 2017/6/20.
 */
public class WriterBolt extends BaseBasicBolt {

    private static final long serialVersionUID = -5653803832498574866L;
    private FileWriter writer = null;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        try {
            writer = new FileWriter("/home/hadoop/storm_kafka_output"+ UUID.randomUUID());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String str = tuple.getString(0);
        try {
            writer.write(str);
            writer.write("\n");
            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
