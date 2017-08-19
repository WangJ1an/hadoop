package wangjian.hadoop.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * Created by 1 on 2017/5/17.
 */

//4个泛型中，前2个是指定Mapper输入数据的类型，KEYIN是输入的key的类型，VALUEIN是输入的value的类型
//map和reduce的数据输入输出都是以key-value的形式封装的
//默认情况下，框架传递给我们的mapper的输入数据中，key是要处理的文本中的一行的起始偏移量，
// 这一行的内容作value

public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    //WordCount mapreduce


    //mapreduce框架每读一行数据就调用一次该方法
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //具体业务逻辑就写在改方法体中，而且业务要处理的数据已被框架传递进来，key-value
        //key是这一行数据的起始偏移量，value是这一行的内容

        String line = value.toString();

        String[] words = StringUtils.split(line,' ');

        for (int i = 0; i < words.length; i++) {
            context.write(new Text(words[i]), new LongWritable(1));
        }

    }
}
