package llyy.enhance;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by 1 on 2017/6/29.
 */
public class LogEnhanceOutPutFormat<K,V> extends FileOutputFormat<K,V>  {

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(new Configuration());
        FSDataOutputStream enhancedOs = fs.create(new Path("D:\\workspace\\hadoop\\src\\llyy\\enhance\\output\\enhance"));
        FSDataOutputStream tocrawlOs = fs.create(new Path("D:\\workspace\\hadoop\\src\\llyy\\enhance\\output\\tocrawl"));
        return new LogEnhanceRecordWriter<K, V>(enhancedOs,tocrawlOs);
    }

    public static class LogEnhanceRecordWriter<K,V> extends RecordWriter<K,V> {

        private FSDataOutputStream enhancedOs;
        private FSDataOutputStream tocrawlOS;

        public LogEnhanceRecordWriter(FSDataOutputStream enhancedOs, FSDataOutputStream tocrawlOs) {
            this.enhancedOs = enhancedOs;
            this.tocrawlOS = tocrawlOs;
        }

        @Override
        public void write(K k, V v) throws IOException, InterruptedException {
          if (k.toString().contains("tocrawl")) {
                tocrawlOS.write(k.toString().getBytes());
            } else {
                enhancedOs.write(k.toString().getBytes());
            }
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

            if (enhancedOs != null) {
                enhancedOs.close();
            }
            if (tocrawlOS != null) {
                tocrawlOS.close();
            }
        }
    }
}
