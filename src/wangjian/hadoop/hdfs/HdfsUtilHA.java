package wangjian.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by 1 on 2017/6/1.
 */
public class HdfsUtilHA {

    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://ns1/"),conf,"hadoop");

        fs.delete(new Path("hdfs://ns1/progit2.pdf"),false);


    }

}
