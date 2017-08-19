package wangjian.hadoop.Hbase;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by 1 on 2017/6/12.
 */
public class HbaseDao {

    @Test
    public void insertTest() throws IOException {

        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum","192.168.2.205:2181," +
                "192.168.2.206:2181," +
                "192.168.2.207:2181");

        HTable nvshen = new HTable(conf,"nvshen");

        Put name = new Put(Bytes.toBytes("rk0001"));
        Put age = new Put(Bytes.toBytes("rk0001"));

        name.add(Bytes.toBytes("base_info"),Bytes.toBytes("name"),Bytes.toBytes("angelbaby"));
        age.add(Bytes.toBytes("base_info"),Bytes.toBytes("age"),Bytes.toBytes("18"));

        ArrayList<Put> puts = new ArrayList<>();
        puts.add(name);
        puts.add(age);

        nvshen.put(puts);
    }

    public static void main(String[] args) throws IOException, ServiceException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","192.168.2.205:2181," +
                "192.168.2.206:2181," +
                "192.168.2.207:2181");

        HBaseAdmin admin = new HBaseAdmin(conf);

        TableName name = TableName.valueOf("nvshen");

        HTableDescriptor desc = new HTableDescriptor(name);

        HColumnDescriptor base_info = new HColumnDescriptor("base_info");
        HColumnDescriptor extra_info = new HColumnDescriptor("extra_info");
        base_info.setMaxVersions(5);

        desc.addFamily(base_info);
        desc.addFamily(extra_info);

        admin.createTable(desc);
    }

}
