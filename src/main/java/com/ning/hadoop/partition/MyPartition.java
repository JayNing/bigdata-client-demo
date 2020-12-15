package com.ning.hadoop.partition;

import com.ning.hadoop.serializable.PhoneMetaBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * ClassName: MyPartition
 * Description:
 * date: 2020/12/15 16:07
 *
 * @author ningjianjian
 */
public class MyPartition extends Partitioner<Text, PhoneMetaBean> {

    @Override
    public int getPartition(Text text, PhoneMetaBean phoneMetaBean, int numPartitions) {
        String phone = text.toString();
        String prefix = phone.substring(0, 3);
        if (prefix.equals("136")){
            return 0;
        } else if (prefix.equals("137")){
            return 1;
        } else if (prefix.equals("138")){
            return 2;
        } else if (prefix.equals("139")){
            return 3;
        } else {
            return 4;
        }
    }
}
