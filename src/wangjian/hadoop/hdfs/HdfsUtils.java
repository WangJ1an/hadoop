package wangjian.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by 1 on 2017/5/13.
 */
public class HdfsUtils {
    FileSystem fs = null;

    @Before
    public void init() throws IOException, URISyntaxException, InterruptedException {
        //读取classpath下的xxx-site.xml 配置文件，并解析其内容，封装到cof对象中
        Configuration conf = new Configuration();

        //也可以在代码中对conf中的配置信息进行手动设置，会覆盖掉原先在配置文件中读取的值
        conf.set("fs.defaultFS","hdfs://MetaServer:9000/");

        //根据配置信息，去获取一个具体文件系统的客户端操作实例对象
        fs = FileSystem.get(new URI("hdfs://MetaServer:9000/"),conf,"wangjian");
    }

    @Test
    public void Test () throws IOException {
        FileChecksum fck = fs.getFileChecksum(new Path("hdfs://MetaServer:9000/hadoop-2.8.0.tar.gz"));

        System.out.println(fck.getAlgorithmName());

        System.out.println(fck.getChecksumOpt().toString());

    }



    //上传文件
    @Test
    public void upload () throws IOException {

        Path dspath = new Path("hdfs://MetaServer:9000/aa/HDFS.md");
        FSDataOutputStream os = fs.create(dspath);
        FileInputStream fis = new FileInputStream("D://学习资料//Hadoop//HDFS.md");
        IOUtils.copyBytes(fis, os, 4096);
    }
    @Test
    public void upload2 () throws IOException {
        fs.copyFromLocalFile(new Path("D://学习资料//Hadoop//HDFS.md"), new Path("hdfs://MetaServer:9000/aaa/HDFS2.md"));
    }

    //下载
    @Test
    public void download () throws IOException {
        fs.copyToLocalFile(new Path("hdfs://MetaServer:9000/aa/HDFS2.md"), new Path("D:\\学习资料\\Hadoop\\download"));
    }

    @Test
    public void download2 () throws IOException {
        FSDataInputStream fis = fs.open(new Path("/aaa/HDFS2.md"));

        FileOutputStream fos = new FileOutputStream(new File("D://学习资料//hdfs.md"));

        IOUtils.copyBytes(fis,fos,1024);
    }

    // 创建文件夹
    @Test
    public void mkdir () throws IOException {

        fs.mkdirs(new Path("/test"));

    }

    //删除文件夹
    @Test
    public void rm () throws IOException {

        fs.delete(new Path("/test/"), true);

    }

    //查看文件信息
    @Test
    public void listFiles () throws IOException {
        //listFiles列出的是文件信息，而且提供递归遍历
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);

        while (files.hasNext()) {
            LocatedFileStatus fileStatus = files.next();
            Path filePath = fileStatus.getPath();  //文件名在Path里面
            String fileName = filePath.getName();
            System.out.println(fileName);
        }

        System.out.println("--------------------------------");

        //listStatus 可以列出文件和文件夹的信息，但不提供自带的递归遍历
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus fs : listStatus) {
            String name = fs.getPath().getName();
            System.out.println(name + (fs.isDirectory() ? " dir " : " file "));
        }
    }

    public static void main(String[] args) {

    }

}
