package com.ning.hadoop.serializable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * ClassName: Phone01Mapper
 * Description:
 * date: 2020/12/10 16:39
 *
 * @author ningjianjian
 */
public class Phone01Mapper extends Mapper<LongWritable, Text, Text, PhoneMetaBean> {

    private Text redKey = new Text();
    private PhoneMetaBean phoneMetaBean = new PhoneMetaBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split("\t");
        String phoneNo = split[1];
        String upFlowString = split[split.length - 3];
        String downFlowString = split[split.length - 2];
        Long upFlow = Long.parseLong(upFlowString);
        Long downFlow = Long.parseLong(downFlowString);

        phoneMetaBean.setUpFlow(upFlow);
        phoneMetaBean.setDownFlow(downFlow);
        phoneMetaBean.setSumFlow(upFlow + downFlow);
        redKey.set(phoneNo);

        context.write(redKey, phoneMetaBean);

    }
}
