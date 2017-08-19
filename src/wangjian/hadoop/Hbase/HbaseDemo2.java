package wangjian.hadoop.Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by 1 on 2017/7/24.
 */
public class HbaseDemo2 {
    private Configuration conf = null;
    private Connection connection = null;
    private Table table = null;

    @Before
    public void init() throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","server1:2181,server2:2181,server3:2181");
        connection = ConnectionFactory.createConnection(conf);
        table = connection.getTable(TableName.valueOf("user"));
    }

    @Test
    public void insertData() throws IOException {
        ArrayList<Put> list = new ArrayList<>();
        Put put = new Put(Bytes.toBytes("200"));
        put.addColumn(Bytes.toBytes("info2"), Bytes.toBytes("name"),Bytes.toBytes("zhangsan"));
        put.addColumn(Bytes.toBytes("info2"), Bytes.toBytes("sex"),Bytes.toBytes("man"));
        put.addColumn(Bytes.toBytes("info2"), Bytes.toBytes("age"),Bytes.toBytes("18"));

        Put put1 = new Put(Bytes.toBytes("2000"));
        put1.addColumn(Bytes.toBytes("info2"), Bytes.toBytes("name"),Bytes.toBytes("haha"));
        put1.addColumn(Bytes.toBytes("info2"), Bytes.toBytes("sex"),Bytes.toBytes("woman"));
        put1.addColumn(Bytes.toBytes("info2"), Bytes.toBytes("age"),Bytes.toBytes("19"));

        list.add(put);
        list.add(put1);
        table.put(list);

    }

    @Test
    public void deleteData() throws IOException {
        Delete delete = new Delete("200".getBytes());
//        delete.addColumn("info2".getBytes(),"name".getBytes());
        table.delete(delete);
    }

    /**
     * 单条查询
     * @throws IOException
     */
    @Test
    public void queryData() throws IOException {
        Get get = new Get("2000".getBytes());
        Result result = table.get(get);
        byte[] value = result.getValue("info2".getBytes(),"name".getBytes());
        System.out.println(new String(value));
    }

    @Test
    public void testScan() throws IOException {
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        Result result = scanner.next();
        byte[] value = result.getValue("info2".getBytes(),"name".getBytes());
        System.out.println(Bytes.toString(value));
    }

    @After
    public void close() throws IOException {
        table.close();
        connection.close();
    }

}
