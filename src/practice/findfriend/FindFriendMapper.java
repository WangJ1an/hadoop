package practice.findfriend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by 1 on 2017/7/5.
 */
public class FindFriendMapper extends Mapper<LongWritable,Text,Text,Text> {
    HashMap<String,String> hashMap = new HashMap<>();
    ArrayList<String> arrayList = new ArrayList<>();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("[,:]");
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i < fields.length; i++) {
            sb.append(fields[i]);
        }
        arrayList.add(fields[0]);
        hashMap.put(fields[0],sb.toString());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (int i = 0; i < arrayList.size(); i++) {
            String user = arrayList.get(i);
            String firend = hashMap.get(user);
            for (int j = i+1; j < arrayList.size(); j++) {
                String secUser = arrayList.get(j);
                String key = user+"-"+secUser;
                String secFriend = hashMap.get(secUser);
                StringBuilder commonFriend;
                if (firend.length() < secFriend.length()) {
                    commonFriend = getCommonFirend(secUser,secFriend,firend);
                } else {
                    commonFriend = getCommonFirend(user,firend,secFriend);
                }
                if (commonFriend.length()!=0) {
                    commonFriend.deleteCharAt(commonFriend.length()-1);
                }
                context.write(new Text(key), new Text(commonFriend.toString()));
            }
        }
    }

    private StringBuilder getCommonFirend(String user_moreFriend,String friend_more,String firend_less) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < firend_less.length(); i++) {
            char ch = firend_less.charAt(i);

            if (user_moreFriend.equals(String.valueOf(ch))) {
                continue;
            }

            if (friend_more.contains(String.valueOf(ch))) {
                sb.append(ch + ",");
            }
        }
        return sb;
    }
}
