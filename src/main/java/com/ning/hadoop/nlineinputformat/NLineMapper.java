package com.ning.hadoop.nlineinputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * ClassName: NLineMapper
 * Description:
 * date: 2020/12/14 17:12
 *
 * @author ningjianjian
 */
public class NLineMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private Text mKey = new Text();
    private LongWritable valLongWritable = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String string = value.toString();
        String[] split = string.split(" ");
        for (String word : split) {
            mKey.set(word);
            context.write(mKey, valLongWritable);
        }

    }
}
