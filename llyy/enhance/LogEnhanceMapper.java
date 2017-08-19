package llyy.enhance;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;

/**
 * 读入原始数据 时间戳。。。。destip srcip。。。url。。。。
 * 抽取其中的url查询规则库中的众多的内容识别信息，网站类别，关键字
 * 将分析结果追加到原始日志后面
 *
 * 如果某条url在规则库中查不到结果，则输出到待爬清单
 *
 * Created by 1 on 2017/6/29.
 */
public class LogEnhanceMapper extends Mapper<LongWritable,Text,Text,NullWritable>{

    private HashMap<String,String> hashMap = new HashMap<>();

    //setup方法是在mapper task初始化时被调用一次
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        DBLoader.dbLoader(hashMap);

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] fields = org.apache.hadoop.util.StringUtils.split(line,'\t');

        try {
            if (fields.length > 27 && !fields[15].isEmpty()
                    && fields[15].startsWith("http")) {
                String url = fields[15];
                String info = hashMap.get(url);
                String result = url;
                if (info != null) {
                    result = result + "\t" + info + "\r\n";
                    context.write(new Text(result),NullWritable.get());

                } else {
                    result = result + "\t" + "tocrawl" + "\r\n";
                    context.write(new Text(result),NullWritable.get());
                }

            }
        }catch (Exception e) {

        }
    }
}
