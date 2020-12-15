package com.ning.hadoop.groupingcomparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * ClassName: OrderGroupingComparator
 * Description:
 * date: 2020/12/15 17:26
 *
 * @author ningjianjian
 */
public class OrderGroupingComparator extends WritableComparator {

    protected OrderGroupingComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;

        return aBean.getId().compareTo(bBean.getId());
    }
}
