package com.ning.hadoop.keyvaluetextinputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * ClassName: KeyValueTextReducer
 * Description:
 * date: 2020/12/14 13:51
 *
 * @author ningjianjian
 */
public class KeyValueTextReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    private LongWritable vaLongWritable = new LongWritable();
    private long sum;

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        sum = 0;
        for (LongWritable value : values) {
            sum += value.get();
        }
        vaLongWritable.set(sum);
        context.write(key, vaLongWritable);
    }
}
