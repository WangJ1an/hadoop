package wangjian.hadoop.Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Created by 1 on 2017/6/12.
 */
public class HbaseDemo {

    private Configuration conf = null;

    @Before
    public void init() {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","weekend05:2181,weekend06:2181,weekend07:2181");
    }

    @Test
    public void testDrop() throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.disableTable("nvshen");
        admin.deleteTable("nvshen");
        admin.close();
    }

    @Test
    public void testPut() throws IOException {
        HTable table = new HTable(conf,"person_info");
        Put p = new Put(Bytes.toBytes("person_0003"));
        p.add(Bytes.toBytes("base_info"),Bytes.toBytes("name"),Bytes.toBytes("wangjian"));

        table.put(p);
        table.close();
    }

    @Test
    public void testGet() throws IOException {
        HTable table = new HTable(conf,"person_info");
        Get get = new Get(Bytes.toBytes("person_0001"));
        get.setMaxVersions(5);
        Result result = table.get(get);
        List<Cell> cells = result.listCells();

//        result.getValue(family,qualifier);可以从result中直接取出一个特定的值

        for (Cell cell : cells) {
            String family = new String(cell.getFamily());
            System.out.println(family);

            String qualifier = new String(cell.getQualifier());
            System.out.println(qualifier);

            String value = new String(cell.getValue());
            System.out.println(value);
        }
        table.close();
    }

    /**
     * 多种过滤条件的使用方法
     *
     */
    @Test
    public void testScan() throws IOException {
        HTable table = new HTable(conf,"person_info");

        //包括开始行，不包括结束行
        Scan scan = new Scan(Bytes.toBytes("person_0001"),Bytes.toBytes("person_0004"));

        //前缀过滤器---针对行键
        Filter filter = new PrefixFilter(Bytes.toBytes("2"));

        //行过滤器
        ByteArrayComparable rowComparator = new BinaryComparator(Bytes.toBytes("person_0003"));
        RowFilter rf = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,rowComparator);

        /**
         * 假设rowkey格式为：创建日期_发布日期_ID_TITLE
         * 目标：查找  发布日期  为  2014-12-21  的数据
         */
        rf = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("_2014-12-21_"));

        //单值过滤器1 完整匹配字节数组
        Filter f = new SingleColumnValueFilter(Bytes.toBytes("base_info"), Bytes.toBytes("name"),
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes("wangjian"));

        //单值过滤器2 匹配正则表达式
        ByteArrayComparable comparable = new RegexStringComparator("z.");
        new SingleColumnValueFilter(Bytes.toBytes("base_info"), Bytes.toBytes("name"),
                CompareFilter.CompareOp.EQUAL, comparable);

        //单值过滤器3 匹配是否包含子串，大小写不敏感
        comparable = new SubstringComparator("ang");
        new SingleColumnValueFilter(Bytes.toBytes("base_info"), Bytes.toBytes("name"),
                CompareFilter.CompareOp.EQUAL, comparable);

        //键值对元数据过滤----family过滤---字节数组完整匹配
        FamilyFilter ff = new FamilyFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes("base_info")));

        ff = new FamilyFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryPrefixComparator(Bytes.toBytes("inf")));
                //表中存在以inf打头的列族，过滤结果为该列族所有行

        //键值对元数据过滤----qualifier过滤---字节数组完整匹配
        filter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes("na")));  //表中不存在na列时过滤结果为空

        filter = new QualifierFilter(
                CompareFilter.CompareOp.EQUAL ,
                new BinaryPrefixComparator(Bytes.toBytes("na"))   //表中存在以na打头的列name，过滤结果为所有行的该列数据
        );

        //基于列名(即Qualifier)前缀过滤数据的ColumnPrefixFilter
        filter = new ColumnPrefixFilter("na".getBytes());

        //基于列名(即Qualifier)多个前缀过滤数据的MultipleColumnPrefixFilter
        byte[][] prefixes = new byte[][] {Bytes.toBytes("na"), Bytes.toBytes("me")};
        filter = new MultipleColumnPrefixFilter(prefixes);

        //为查询设置过滤条件
        scan.setFilter(filter);

        scan.addFamily(Bytes.toBytes("base_info"));
        ResultScanner scanner = table.getScanner(scan);
        for (Result r : scanner) {
          /*  for (KeyValue kv : r.list()) {
                String family = new String(kv.getFamily());
                System.out.println(family);
                String qualifier = new String(kv.getQualifier());
                System.out.println(qualifier);
                System.out.println(new String(kv.getValue()));
            }*/
          //直接从result中取到某个特定的value
            byte[] value = r.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("name"));
            System.out.println(new String(value));
        }
        table.close();

    }

    @Test
    public void testDel() throws IOException {
        HTable table = new HTable(conf,"person_info");
        Delete del = new Delete(Bytes.toBytes("person_0002"));
        table.delete(del);
        table.close();
    }


    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","weekend05:2181,weekend06:2181,weekend07:2181");;

        HBaseAdmin admin = new HBaseAdmin(conf);
        TableName tableName = TableName.valueOf("person_info");
        HTableDescriptor person_info = new HTableDescriptor(tableName);
        HColumnDescriptor base_info = new HColumnDescriptor("base_info");

        base_info.setMaxVersions(10);
        person_info.addFamily(base_info);

        admin.createTable(person_info);
        admin.close();
    }
}
