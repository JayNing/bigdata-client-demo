package com.ning.hadoop.writablecomparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * ClassName: WritableComparableReducer
 * Description:
 * date: 2020/12/15 16:39
 *
 * @author ningjianjian
 */
public class WritableComparableReducer extends Reducer<PhoneMetaV2Bean, Text, Text, PhoneMetaV2Bean> {
    @Override
    protected void reduce(PhoneMetaV2Bean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value, key);
        }
    }
}
