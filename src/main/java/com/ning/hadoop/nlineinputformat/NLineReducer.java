package com.ning.hadoop.nlineinputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * ClassName: NLineReducer
 * Description:
 * date: 2020/12/14 17:14
 *
 * @author ningjianjian
 */
public class NLineReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    private long sum;
    private LongWritable val = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        sum = 0L;

        for (LongWritable value : values) {
            sum += value.get();
        }
        val.set(sum);
        context.write(key,val);
    }
}
