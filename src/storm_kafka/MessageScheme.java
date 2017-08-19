package storm_kafka;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * Created by 1 on 2017/6/20.
 */
public class MessageScheme implements Scheme {
    @Override
    public List<Object> deserialize(byte[] bytes) {
        try {
            String msg = new String(bytes,"UTF-8");
            return new Values(msg);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("msg");
    }
}
