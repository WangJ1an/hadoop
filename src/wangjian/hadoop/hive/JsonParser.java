package wangjian.hadoop.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Created by 1 on 2017/7/16.
 */
public class JsonParser extends UDF {
    public String evaluate(String jsonLine) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            MovieRateBean bean = objectMapper.readValue(jsonLine,MovieRateBean.class);
            return bean.toString();
        } catch (Exception e) {

        }
        return "";
    }
}
