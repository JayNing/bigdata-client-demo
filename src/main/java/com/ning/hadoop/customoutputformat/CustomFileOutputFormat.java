package com.ning.hadoop.customoutputformat;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

/**
 * ClassName: CustomFileOutputFormat
 * Description:
 * date: 2020/12/16 17:05
 *
 * @author ningjianjian
 */
public class CustomFileOutputFormat extends FileOutputFormat<Text, NullWritable> {

    @Override
    public org.apache.hadoop.mapreduce.RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        return new CustomRecordWriter(job);
    }
}
