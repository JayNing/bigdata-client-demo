package com.ning.hadoop.writablecomparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * ClassName: WritabComparableMapper
 * Description:
 * date: 2020/12/15 16:35
 *
 * @author ningjianjian
 */
public class WritableComparableMapper extends Mapper<LongWritable, Text, PhoneMetaV2Bean, Text> {

    private PhoneMetaV2Bean phoneMetaV2Bean = new PhoneMetaV2Bean();
    private Text outputText = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        String phoneNo = split[0];
        String upFlowString = split[1];
        String downFlowString = split[2];
        Long upFlow = Long.parseLong(upFlowString);
        Long downFlow = Long.parseLong(downFlowString);

        phoneMetaV2Bean.setUpFlow(upFlow);
        phoneMetaV2Bean.setDownFlow(downFlow);
        phoneMetaV2Bean.setSumFlow(Long.parseLong(split[3]));

        outputText.set(phoneNo);
        context.write(phoneMetaV2Bean, outputText);
    }
}
