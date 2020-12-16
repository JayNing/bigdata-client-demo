package com.ning.hadoop.customoutputformat;


import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * ClassName: CustomRecordWriter
 * Description:
 * date: 2020/12/16 17:07
 *
 * @author ningjianjian
 */
public class CustomRecordWriter extends RecordWriter<Text, NullWritable> {

    FSDataOutputStream atguiguOut = null;
    FSDataOutputStream otherOut = null;

    public CustomRecordWriter(TaskAttemptContext job) {
        FileSystem fs = null;
        try {
            // 1 获取fs
            fs = FileSystem.get(job.getConfiguration());

            // 2 创建输出文件路径
            Path atguiguPath = new Path("C:\\Users\\ningjianjian\\Desktop\\wc\\customoutputformat\\atguigu.log");
            Path otherPath = new Path("C:\\Users\\ningjianjian\\Desktop\\wc\\customoutputformat\\other.log");

            // 3 创建输出流
            atguiguOut = fs.create(atguiguPath);
            otherOut = fs.create(otherPath);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void write(Text key, NullWritable value) throws IOException {
        String msg = key.toString();
        if (msg.contains("atguigu")){
            atguiguOut.write(msg.getBytes());
        } else {
            otherOut.write(msg.getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(atguiguOut);
        IOUtils.closeStream(otherOut);
    }

}
