package com.ning.hadoop.custominputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * ClassName: CustomReducer
 * Description:
 * date: 2020/12/14 19:09
 *
 * @author ningjianjian
 */
public class CustomReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key,values.iterator().next());
    }
}
