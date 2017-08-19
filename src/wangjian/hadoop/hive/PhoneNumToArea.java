package wangjian.hadoop.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.HashMap;

/**
 * Created by 1 on 2017/6/11.
 */
public class PhoneNumToArea extends UDF{
    private static HashMap<String, String> areaMap = new HashMap<>();

    static {
        areaMap.put("1388", "beijing");
        areaMap.put("1399", "tainjing");
        areaMap.put("1366", "nanjing");
    }

    //一定要用public修饰才能被hive调用
    public String evaluate(String phoneNumber) {
        String str = phoneNumber.substring(0,4);
        String result = areaMap.get(str) == null ? ("phonenumber:Mars") : areaMap.get(str);
        return result;
    }

}
