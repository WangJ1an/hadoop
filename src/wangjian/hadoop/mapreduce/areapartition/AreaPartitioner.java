package wangjian.hadoop.mapreduce.areapartition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import wangjian.hadoop.mapreduce.flowsum.FlowBean;

import java.util.HashMap;

/**
 * Created by 1 on 2017/5/18.
 */
public class AreaPartitioner extends Partitioner<Text,FlowBean> {

    private static HashMap<String, Integer> areaMap = new HashMap<>();

    static {
        areaMap.put("130",0);
        areaMap.put("131",1);
        areaMap.put("132",2);
        areaMap.put("133",3);
        areaMap.put("134",4);
    }


    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        Integer areaCoder = areaMap.get(text.toString().substring(0,3));
        areaCoder = areaCoder == null ? 5 : areaCoder;
        return areaCoder;
    }
}
