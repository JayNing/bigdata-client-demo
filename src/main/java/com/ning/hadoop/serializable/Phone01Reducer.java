package com.ning.hadoop.serializable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * ClassName: Phone01Reducer
 * Description:
 * date: 2020/12/10 16:44
 *
 * @author ningjianjian
 */
public class Phone01Reducer extends Reducer<Text, PhoneMetaBean, Text, PhoneMetaBean> {

    long upFlow;
    long downFlow;
    long sumFlow;

    @Override
    protected void reduce(Text key, Iterable<PhoneMetaBean> values, Context context) throws IOException, InterruptedException {

        upFlow = 0;
        downFlow = 0;
        sumFlow = 0;

        for (PhoneMetaBean value : values) {
            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
            sumFlow += value.getSumFlow();
        }
        PhoneMetaBean outBean = new PhoneMetaBean();
        outBean.setUpFlow(upFlow);
        outBean.setDownFlow(downFlow);
        outBean.setSumFlow(sumFlow);

        context.write(key, outBean);
    }
}
