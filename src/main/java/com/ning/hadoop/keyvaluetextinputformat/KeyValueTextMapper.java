package com.ning.hadoop.keyvaluetextinputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * ClassName: KeyValueTextMapper
 * Description:
 * date: 2020/12/14 13:47
 *
 * @author ningjianjian
 */
public class KeyValueTextMapper extends Mapper<Text, Text, Text, LongWritable> {

    private LongWritable longWritable = new LongWritable(1);

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key, longWritable);
    }
}
