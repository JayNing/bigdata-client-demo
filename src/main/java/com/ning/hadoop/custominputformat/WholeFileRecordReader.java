package com.ning.hadoop.custominputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * ClassName: WholeFileRecordReader
 * Description:
 * date: 2020/12/14 18:43
 *
 * @author ningjianjian
 */
public class WholeFileRecordReader extends RecordReader<Text, BytesWritable> {

    private Text key = new Text();
    private BytesWritable value = new BytesWritable();
    private FileSplit fileSplit;
    private Configuration configuration;
    private boolean isProgress = true;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) split;
        this.configuration = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (isProgress){
            byte[] buff = new byte[(int) fileSplit.getLength()];
            FileSystem fs = null;
            FSDataInputStream fis = null;

            try {
                Path path = fileSplit.getPath();
                fs = path.getFileSystem(configuration);
                fis = fs.open(path);
                IOUtils.readFully(fis, buff,0, buff.length);
                value.set(buff,0, buff.length);

                String keyName = path.toString();
                key.set(keyName);

            }catch (Exception e) {
                e.printStackTrace();
            }finally {
                IOUtils.closeStream(fis);
            }
            isProgress = false;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
