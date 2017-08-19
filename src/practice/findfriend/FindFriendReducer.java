package practice.findfriend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by 1 on 2017/7/5.
 */
public class FindFriendReducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text text : values) {
            String friend = text.toString();
            context.write(new Text(key+":"),new Text(friend));
        }
    }
}