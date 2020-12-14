package com.ning.hadoop.custominputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * ClassName: CustomMapper
 * Description:
 * date: 2020/12/14 19:08
 *
 * @author ningjianjian
 */
public class CustomMapper extends Mapper<Text, BytesWritable, Text, BytesWritable> {
    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }
}
