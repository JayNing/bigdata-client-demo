package com.ning.hadoop.customoutputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * ClassName: CustomOutputFormatMapper
 * Description:
 * date: 2020/12/16 22:00
 *
 * @author ningjianjian
 */
public class CustomOutputFormatMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(value, NullWritable.get());
    }
}
